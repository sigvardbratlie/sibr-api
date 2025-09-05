# SIBR API


## Overview

This Python module provides a base class, ApiBase, designed to simplify the process of building robust, asynchronous API clients. It handles common challenges such as session management, concurrency limiting, rate-limiting, error handling, and periodic saving of results, allowing developers to focus on the specific logic of the API they are integrating with.

The framework is built on aiohttp for asynchronous HTTP requests and is structured as a class, requiring the user to implement API-specific logic for fetching, transforming, and saving data.


## ✨ Key Features



* **Asynchronous by Design**: Utilizes asyncio and aiohttp for high-performance, non-blocking API calls.
* **Concurrency Control**: Employs an asyncio.Semaphore to limit the number of concurrent requests, preventing the client from overwhelming the API server.
* **Built-in Error Handling**: Gracefully handles common HTTP errors (e.g., 429 Too Many Requests, 401 Unauthorized, 403 Forbidden, 404 Not Found, 500 Internal Server Error) and raises custom, specific exceptions (RateLimitError, APIkeyError, NotFoundError, SkipItemException).
* **Flexible Structure**: The ApiBase class can be easily extended to implement API-specific logic for fetching, transforming, and saving data.
* **Automatic Batch Processing**: Manages the processing of large lists of inputs, yielding results as they are completed.
* **Periodic Saving**: Includes functionality to automatically save results at specified intervals, preventing data loss during long-running jobs.
* **Proxy Support**: Easily configurable to route requests through HTTP/HTTPS proxies.


## ⚙️ Installation & Prerequisites

To get startet, run the pip install command:

```bash
pip install sibr-api
```

## How to use

To use this module, you must create a new class that inherits from ApiBase and implement the necessary methods for your specific API integration.


### Step 1: Create a Subclass

First, define a class that inherits from ApiBase. In the __init__ method, you should call the parent constructor and set the base_url for the API you are targeting.

```python
from sibr_api.base import ApiBase
import pandas as pd

class MyApiClient(ApiBase):
    def __init__(self):
        super().__init__(logger_name='MyApiClient')
        self.base_url = "[https://api.example.com/v1](https://api.example.com/v1)"
```

### Step 2: Implement Your Logic

You must provide concrete implementations for the following methods in your subclass.

**1. fetcher(self, item)**

This async method defines how to fetch a single item from the API. It takes one argument, item, which contains the necessary information (e.g., an ID, a search query) to build the request URL. Inside this method, you should:


* Construct the full request URL.
* Call await self.fetch_single(url) to perform the request.

```python
async def get_item(self, item_id: str):
    """ 
    Fetches data for a single item_id. 
    """ 
    endpoint = f"/data/{item_id}"
    url = self.base_url + endpoint
 
    raw_response = await self.fetch_single(url)
 
    if raw_response:
        self.ok_responses += 1
        return raw_response
    else:
        self.fail_responses += 1
        return None
```

**2. transformer(self, output)**

This method is for processing the raw JSON response from the API into a more usable format. For example, you can extract relevant fields, flatten a nested structure, or convert it into a specific object.

```python
def transform_output(self, output: list) -> pd.DataFrame: 
    """ 
    Transforms the raw API response into a clean DataFrame. 
    """ 
    # Example: Extracting specific fields from the JSON response 
    df = pd.DataFrame(output) 
    return df
```

**3. saver(self, results)**

This method defines how to persist a list of processed results. It is called automatically by the framework when the save_interval is reached. A common implementation is to save the data to a CSV or JSON file.
```python
def save_func(self, results: pd.DataFrame):
    """
    Saves a DataFrame to a CSV file.
    """
    # Use mode='a' to append to the file, and header=not os.path.exists(path)
    # to write the header only once. 
    results.to_csv('results.csv', mode='a', header=False, index=False)
```


### Step 3: Run the Client

Once your class is defined, you can instantiate it and use the get_items_with_ids or get_items methods to fetch data in bulk. These methods should be run within an async function.

```python

import asyncio 
import pandas as pd 
import os 
 
# Assuming the MyApiClient class from above is defined here 
 
async def main(): 
    # A list of item IDs to fetch 
    item_ids_to_fetch = [f"id_{i}" for i in range(100)] 
 
    # Instantiate the client 
    client = MyApiClient() 
 
    print("Starting API calls...") 
 
    # Fetch all items, with a maximum of 10 concurrent requests 
    # The results will be saved to 'results.csv' every 50 items. 
    all_results = await client.get_items_with_ids( 
        inputs=item_ids_to_fetch, 
        fetcher=client.get_item, 
        transformer=client.transform_output, 
        saver=client.save_func, 
        save_interval=50, 
        concurrent_requests=10 
    ) 
 
    # Ensure the session is closed gracefully 
    await client.close() 
 
    print(f"Finished. Total results processed: {len(all_results)}") 
    # The `all_results` variable contains the transformed data. 
 
if __name__ == "__main__": 
    # Initialize the results file with a header before starting 
    if not os.path.exists('results.csv'): 
        pd.DataFrame(columns=['id', 'name', 'value']).to_csv('results.csv', index=False) \
 
    asyncio.run(main()) 

```


## Class References


### ApiBase

The base class for creating an API client.


#### Public Methods



* async def get_items(self, inputs: list, fetcher: Callable, transformer: Callable, saver: Callable = None, save_interval: int = 50000, concurrent_requests: int = 5) -> list \
Fetches multiple items concurrently from a list of inputs. \
**Returns**: A list of processed results.
* async def get_items_with_ids(self, inputs: list | dict, fetcher: Callable, transformer: Callable, saver: Callable = None, save_interval: int = 50000, concurrent_requests: int = 5) -> list \
Similar to get_items, but associates each result with its original input ID. \
**Returns**: A list of tuples, where each tuple is (item_id, result).
* async def close(self) \
Closes the underlying aiohttp.ClientSession. It's important to call this when you are done to release resources.


#### ⚠️ Error Handling

The module will automatically handle network errors and common HTTP status codes.

* If a 429 Too Many Requests status is received, a RateLimitError is raised, and processing will stop.
* If a 401 Unauthorized status is received, an APIkeyError is raised.
* If a 403 Forbidden status is received, a PermissionError is raised.
* If a 404 Not Found status is received, a NotFoundError is raised.
* If a 500 Internal Server Error status is received, a SkipItemException is raised.
* Other client or server errors are logged, and the corresponding item will have a result of None.