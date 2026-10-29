"""Ephemeral secure single-node HDFS and MIT KDC. Never use this config in production."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

ROOT = Path("/tmp/hrw-cluster")
REALM = "WRITER.TEST"


def run(*args, **kwargs):
    return subprocess.run(args, check=True, timeout=90, **kwargs)


def xml(path, values):
    root = Element("configuration")
    for key, value in values.items():
        prop = SubElement(root, "property")
        SubElement(prop, "name").text = key
        SubElement(prop, "value").text = str(value)
    path.write_bytes(tostring(root))


def main():
    ROOT.mkdir(mode=0o755)
    conf = ROOT / "conf"
    conf.mkdir()
    shutil.copyfile("/opt/hadoop/etc/hadoop/log4j.properties", conf / "log4j.properties")
    os.environ.update(
        {
            "KRB5_CONFIG": str(ROOT / "krb5.conf"),
            "KRB5_KDC_PROFILE": str(ROOT / "kdc.conf"),
            "HADOOP_CONF_DIR": str(conf),
            "HADOOP_HEAPSIZE_MAX": "256",
            "HADOOP_OPTS": f"-Djava.security.krb5.conf={ROOT}/krb5.conf",
        }
    )
    (ROOT / "krb5.conf").write_text(f"""[libdefaults]
 default_realm = {REALM}
 dns_lookup_realm = false
 dns_lookup_kdc = false
 rdns = false
 udp_preference_limit = 1
[realms]
 {REALM} = {{
  kdc = hadoop:88
 }}
[domain_realm]
 hadoop = {REALM}
""")
    (ROOT / "kdc.conf").write_text(f"""[kdcdefaults]
 kdc_ports = 88
 kdc_tcp_ports = 88
[realms]
 {REALM} = {{
  database_name = {ROOT}/principal
  key_stash_file = {ROOT}/stash
  acl_file = {ROOT}/kadm5.acl
  max_life = 2m
  max_renewable_life = 10m
 }}
""")
    run("kdb5_util", "create", "-s", "-P", "test-only-ephemeral-master", "-r", REALM)
    for principal, file in [("hdfs/hadoop", "hdfs"), ("HTTP/hadoop", "http"), ("writer", "writer")]:
        run("kadmin.local", "-r", REALM, "-q", f"addprinc -randkey {principal}@{REALM}")
        run("kadmin.local", "-r", REALM, "-q", f"ktadd -k {ROOT}/{file}.keytab {principal}@{REALM}")
    os.chown(ROOT / "writer.keytab", 10001, 10001)
    os.chmod(ROOT / "writer.keytab", 0o400)
    run(
        "keytool",
        "-genkeypair",
        "-alias",
        "server",
        "-keyalg",
        "RSA",
        "-storetype",
        "JKS",
        "-keystore",
        str(ROOT / "server.jks"),
        "-storepass",
        "test-only-password",
        "-keypass",
        "test-only-password",
        "-dname",
        "CN=hadoop",
        "-validity",
        "2",
        "-ext",
        "SAN=dns:hadoop",
    )
    xml(
        conf / "core-site.xml",
        {
            "fs.defaultFS": "hdfs://hadoop:9000",
            "hadoop.security.authentication": "kerberos",
            "hadoop.security.authorization": "true",
            "hadoop.ssl.server.conf": "ssl-server.xml",
            "hadoop.ssl.client.conf": "ssl-client.xml",
        },
    )
    xml(
        conf / "hdfs-site.xml",
        {
            "dfs.namenode.name.dir": f"file://{ROOT}/nn",
            "dfs.datanode.data.dir": f"file://{ROOT}/dn",
            "dfs.replication": "1",
            "dfs.datanode.synconclose": "true",
            "dfs.namenode.kerberos.principal": f"hdfs/hadoop@{REALM}",
            "dfs.namenode.keytab.file": f"{ROOT}/hdfs.keytab",
            "dfs.datanode.kerberos.principal": f"hdfs/hadoop@{REALM}",
            "dfs.datanode.keytab.file": f"{ROOT}/hdfs.keytab",
            "dfs.namenode.kerberos.internal.spnego.principal": f"HTTP/hadoop@{REALM}",
            "dfs.web.authentication.kerberos.principal": f"HTTP/hadoop@{REALM}",
            "dfs.web.authentication.kerberos.keytab": f"{ROOT}/http.keytab",
            "dfs.http.policy": "HTTPS_ONLY",
            "dfs.namenode.https-address": "hadoop:9871",
            "dfs.datanode.https-address": "hadoop:9865",
            "dfs.datanode.address": "hadoop:9866",
            "dfs.datanode.ipc.address": "hadoop:9867",
            "dfs.data.transfer.protection": "authentication",
            "dfs.block.access.token.enable": "true",
            "dfs.client.use.datanode.hostname": "true",
            "dfs.namenode.safemode.min.datanodes": "1",
        },
    )
    xml(
        conf / "ssl-server.xml",
        {
            "ssl.server.keystore.location": f"{ROOT}/server.jks",
            "ssl.server.keystore.password": "test-only-password",
            "ssl.server.keystore.keypassword": "test-only-password",
            "ssl.server.truststore.location": f"{ROOT}/server.jks",
            "ssl.server.truststore.password": "test-only-password",
        },
    )
    xml(
        conf / "ssl-client.xml",
        {
            "ssl.client.truststore.location": f"{ROOT}/server.jks",
            "ssl.client.truststore.password": "test-only-password",
        },
    )
    processes, logs = [], []
    try:
        for command, name in [(["krb5kdc", "-n"], "kdc")]:
            log = (ROOT / f"{name}.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen(command, stdout=log, stderr=log))
        with (ROOT / "format.log").open("w") as log:
            run("hdfs", "namenode", "-format", "-force", "-nonInteractive", stdout=log, stderr=log)
        for daemon in ("namenode", "datanode"):
            log = (ROOT / f"{daemon}.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen(["hdfs", daemon], stdout=log, stderr=log))
        run("kinit", "-kt", str(ROOT / "hdfs.keytab"), f"hdfs/hadoop@{REALM}")
        for _ in range(60):
            ready = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True, timeout=15)
            if ready.returncode == 0 and b"Live datanodes (1)" in ready.stdout:
                break
            if any(process.poll() is not None for process in processes):
                raise RuntimeError("Test daemon stopped")
            time.sleep(1)
        else:
            raise RuntimeError("HDFS did not become ready")
        run("hdfs", "dfsadmin", "-safemode", "leave")
        run("hdfs", "dfs", "-mkdir", "-p", "/writer")
        run("hdfs", "dfs", "-chown", "writer", "/writer")
        run("runuser", "-u", "writer", "--", "/opt/venv/bin/python", "/integration/check.py")
    except BaseException:
        for name in ("kdc", "format", "namenode", "datanode"):
            path = ROOT / f"{name}.log"
            if path.exists():
                print(f"{name}:\n{path.read_text()[-10000:]}", flush=True)
        raise
    finally:
        for process in reversed(processes):
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
