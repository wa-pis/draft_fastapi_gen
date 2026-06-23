# petstore_client.StoreApi

All URIs are relative to */api/v3*

Method | HTTP request | Description
------------- | ------------- | -------------
[**delete_order**](StoreApi.md#delete_order) | **DELETE** /store/order/{orderId} | Delete purchase order by identifier.
[**get_inventory**](StoreApi.md#get_inventory) | **GET** /store/inventory | Returns pet inventories by status.
[**get_order_by_id**](StoreApi.md#get_order_by_id) | **GET** /store/order/{orderId} | Find purchase order by ID.
[**place_order**](StoreApi.md#place_order) | **POST** /store/order | Place an order for a pet.


# **delete_order**
> delete_order(order_id)

Delete purchase order by identifier.

For valid response try integer IDs with value < 1000. Anything above 1000 or non-integers will generate API errors.

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
    api_instance = petstore_client.StoreApi(api_client)
    order_id = 56 # int | ID of the order that needs to be deleted

    try:
        # Delete purchase order by identifier.
        api_instance.delete_order(order_id)
    except Exception as e:
        print("Exception when calling StoreApi->delete_order: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **order_id** | **int**| ID of the order that needs to be deleted | 

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
**200** | order deleted |  -  |
**400** | Invalid ID supplied |  -  |
**404** | Order not found |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_inventory**
> Dict[str, int] get_inventory()

Returns pet inventories by status.

Returns a map of status codes to quantities.

### Example

* Api Key Authentication (api_key):

```python
import petstore_client
from petstore_client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to /api/v3
# See configuration.py for a list of all supported configuration parameters.
configuration = petstore_client.Configuration(
    host = "/api/v3"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with petstore_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = petstore_client.StoreApi(api_client)

    try:
        # Returns pet inventories by status.
        api_response = api_instance.get_inventory()
        print("The response of StoreApi->get_inventory:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoreApi->get_inventory: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

**Dict[str, int]**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_order_by_id**
> Order get_order_by_id(order_id)

Find purchase order by ID.

For valid response try integer IDs with value <= 5 or > 10. Other values will generate exceptions.

### Example


```python
import petstore_client
from petstore_client.models.order import Order
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
    api_instance = petstore_client.StoreApi(api_client)
    order_id = 56 # int | ID of order that needs to be fetched

    try:
        # Find purchase order by ID.
        api_response = api_instance.get_order_by_id(order_id)
        print("The response of StoreApi->get_order_by_id:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoreApi->get_order_by_id: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **order_id** | **int**| ID of order that needs to be fetched | 

### Return type

[**Order**](Order.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, application/xml

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid ID supplied |  -  |
**404** | Order not found |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **place_order**
> Order place_order(order=order)

Place an order for a pet.

Place a new order in the store.

### Example


```python
import petstore_client
from petstore_client.models.order import Order
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
    api_instance = petstore_client.StoreApi(api_client)
    order = petstore_client.Order() # Order |  (optional)

    try:
        # Place an order for a pet.
        api_response = api_instance.place_order(order=order)
        print("The response of StoreApi->place_order:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling StoreApi->place_order: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **order** | [**Order**](Order.md)|  | [optional] 

### Return type

[**Order**](Order.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json, application/xml, application/x-www-form-urlencoded
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |
**400** | Invalid input |  -  |
**422** | Validation exception |  -  |
**0** | Unexpected error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

