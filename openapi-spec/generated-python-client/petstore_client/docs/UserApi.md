# petstore_client.UserApi

All URIs are relative to */api/v3*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_user**](UserApi.md#create_user) | **POST** /user | Create user.
[**create_users_with_list_input**](UserApi.md#create_users_with_list_input) | **POST** /user/createWithList | Creates list of users with given input array.
[**delete_user**](UserApi.md#delete_user) | **DELETE** /user/{username} | Delete user resource.
[**get_user_by_name**](UserApi.md#get_user_by_name) | **GET** /user/{username} | Get user by user name.
[**login_user**](UserApi.md#login_user) | **GET** /user/login | Logs user into the system.
[**logout_user**](UserApi.md#logout_user) | **GET** /user/logout | Logs out current logged in user session.
[**update_user**](UserApi.md#update_user) | **PUT** /user/{username} | Update user resource.


# **create_user**
> User create_user(user=user)

Create user.

This can only be done by the logged in user.

### Example


```python
import petstore_client
from petstore_client.models.user import User
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)
    user = petstore_client.User() # User | Created user object (optional)

    try:
        # Create user.
        api_response = api_instance.create_user(user=user)
        print("The response of UserApi->create_user:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling UserApi->create_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user** | [**User**](User.md)| Created user object | [optional] 

### Return type

[**User**](User.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json, application/xml, application/x-www-form-urlencoded
 - **Accept**: application/json, application/xml

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_users_with_list_input**
> User create_users_with_list_input(user=user)

Creates list of users with given input array.

Creates list of users with given input array.

### Example


```python
import petstore_client
from petstore_client.models.user import User
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)
    user = [petstore_client.User()] # List[User] |  (optional)

    try:
        # Creates list of users with given input array.
        api_response = api_instance.create_users_with_list_input(user=user)
        print("The response of UserApi->create_users_with_list_input:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling UserApi->create_users_with_list_input: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user** | [**List[User]**](User.md)|  | [optional] 

### Return type

[**User**](User.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json, application/xml

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful operation |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_user**
> delete_user(username)

Delete user resource.

This can only be done by the logged in user.

### Example


```python
import petstore_client
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)
    username = 'username_example' # str | The name that needs to be deleted

    try:
        # Delete user resource.
        api_instance.delete_user(username)
    except Exception as e:
        print("Exception when calling UserApi->delete_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **username** | **str**| The name that needs to be deleted | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | User deleted |  -  |
**400** | Invalid username supplied |  -  |
**404** | User not found |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_user_by_name**
> User get_user_by_name(username)

Get user by user name.

Get user detail based on username.

### Example


```python
import petstore_client
from petstore_client.models.user import User
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)
    username = 'username_example' # str | The name that needs to be fetched. Use user1 for testing

    try:
        # Get user by user name.
        api_response = api_instance.get_user_by_name(username)
        print("The response of UserApi->get_user_by_name:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling UserApi->get_user_by_name: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **username** | **str**| The name that needs to be fetched. Use user1 for testing | 

### Return type

[**User**](User.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, application/xml

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid username supplied |  -  |
**404** | User not found |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **login_user**
> str login_user(username=username, password=password)

Logs user into the system.

Log into the system.

### Example


```python
import petstore_client
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)
    username = 'username_example' # str | The user name for login (optional)
    password = 'password_example' # str | The password for login in clear text (optional)

    try:
        # Logs user into the system.
        api_response = api_instance.login_user(username=username, password=password)
        print("The response of UserApi->login_user:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling UserApi->login_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **username** | **str**| The user name for login | [optional] 
 **password** | **str**| The password for login in clear text | [optional] 

### Return type

**str**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/xml, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  * X-Rate-Limit - calls per hour allowed by the user <br>  * X-Expires-After - date in UTC when token expires <br>  |
**400** | Invalid username/password supplied |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **logout_user**
> logout_user()

Logs out current logged in user session.

Log user out of the system.

### Example


```python
import petstore_client
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)

    try:
        # Logs out current logged in user session.
        api_instance.logout_user()
    except Exception as e:
        print("Exception when calling UserApi->logout_user: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_user**
> update_user(username, user=user)

Update user resource.

This can only be done by the logged in user.

### Example


```python
import petstore_client
from petstore_client.models.user import User
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)


# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.UserApi(api_client)
    username = 'username_example' # str | name that need to be deleted
    user = petstore_client.User() # User | Update an existent user in the store (optional)

    try:
        # Update user resource.
        api_instance.update_user(username, user=user)
    except Exception as e:
        print("Exception when calling UserApi->update_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **username** | **str**| name that need to be deleted | 
 **user** | [**User**](User.md)| Update an existent user in the store | [optional] 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json, application/xml, application/x-www-form-urlencoded
 - **Accept**: Not defined

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | bad request |  -  |
**404** | user not found |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

