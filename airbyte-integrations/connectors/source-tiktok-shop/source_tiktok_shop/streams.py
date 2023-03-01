#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
import json
import math
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
import urllib.parse
import hmac
import hashlib
import base64
import time

class TiktokException(Exception):
    """default exception for custom Tiktok logic"""

def get_current_timeframe():
        # using time module    
        # ts stores the time in seconds    
        ts = time.time()
        # print the current timestamp    
        return round(ts)

def get_tiktok_sign(path, params, app_secret):
    #https://partner.tiktokshop.com/doc/page/274638
    # app_secret = "d797417ec3964f18b30b5a1198abf00b0b8f4188"
    # parsed = urllib.parse.urlparse(url)
    # params = urllib.parse.parse_qsl(parsed.query)
    # params.sort()
    param_concat = ""
    
    for x in sorted(params):
        if x != "sign" and x != "access_token":
            param_concat = param_concat + x + str(params[x])
    #Append the request path to the beginning    
    param_concat = path + param_concat
    param_concat = app_secret + param_concat + app_secret
    print (param_concat)

    signature = hmac.new(
        bytes(app_secret, 'utf8'),
        bytes(param_concat, 'utf8'), 
        digestmod=hashlib.sha256).hexdigest()
    return signature

def sign_request_params(path, params, app_key, app_secret):
    timestamp = get_current_timeframe()

    sign = get_tiktok_sign(path, {
        **params,
        "app_key": app_key,
        "timestamp": str(timestamp)
    }, app_secret)

    result = {
        **params,
        "timestamp": str(timestamp),
        "sign": sign
    }
    return result

# Basic full refresh stream
class TiktokShopStream(HttpStream, ABC):
    # TODO: Fill in the url base. Required.
    url_base = "https://open-api.tiktokglobalshop.com"

    response_list_field = "list"

    def __init__(self, config, **kwargs):
        super().__init__(kwargs)
        self.app_key = config["app_key"]
        self.app_secret = config["app_secret"]
        self.access_token = config["access_token"]
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return ""
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()

        if "data" not in decoded_response or "next_cursor" not in decoded_response.get("data") or decoded_response.get('data').get('more') == False:
            return None
        cursor = decoded_response.get("data").get("next_cursor")
        return {
            "cursor": cursor
        }
    
    def sign_request_params(self, params):
        return sign_request_params(self.path(), params, self.app_key, self.app_secret)

    def get_init_request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None):
        return {
            "app_key": self.app_key,
            "access_token": self.access_token,
        }
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = self.get_init_request_params(stream_state, stream_slice)

        if next_page_token != None:
            params = {
                **params,
                **next_page_token,
            }

        result = self.sign_request_params(params)
        return result

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        if data["code"]:    
            raise TiktokException(data)
        data = data["data"]
        if self.response_list_field in data:
            data = data[self.response_list_field]
            for record in data:
                yield record

class ActiveShops(TiktokShopStream):
    primary_key = "shop_id"
    response_list_field = "active_shops"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/seller/global/active_shops"

class OrderList(TiktokShopStream):
    http_method = "POST"
    primary_key = "order_id"
    response_list_field = "order_list"
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/orders/search"

    def get_init_request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None):
        params = super().get_init_request_params(stream_state, stream_slice)
        return {
            **params,
            "page_size": 50
        }

class OrderDetails(TiktokShopStream):
    http_method = "POST"
    response_list_field = "order_list"
    primary_key = "order_id"
    url_base = "https://open-api.tiktokglobalshop.com"

    def __init__(self, **kwargs):
        super().__init__(config = kwargs.get('config'))
        parent = kwargs.get('parent')
        self.parent = parent
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None;
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/orders/detail/query"
    
    def stream_slices(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_slices = self.parent.stream_slices(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state
        )

        # iterate over parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(
                sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )

            order_ids = []
            # iterate over all parent records with current stream_slice
            for record in parent_records:
                order_ids.append(record.get("order_id"))
            
            total_order = len(order_ids)
            for i in range (math.floor(total_order/50)):
                yield  {
                    "order_ids": order_ids[i:min(i+50, total_order-1)]
                }   
            
    
    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Union[Mapping, str]]:
        order_ids = stream_slice.get('order_ids')
        body = {
            "order_id_list": order_ids
        }
        return body

class ProductList(TiktokShopStream):
    http_method = "POST"
    primary_key = "id"
    response_list_field = "products"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/products/search"

    def get_init_request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None):
        params = super().get_init_request_params(stream_state, stream_slice)
        return {
            "page_number": 1,
            **params,
            "page_size": 50
        }
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        if "data" not in decoded_response or "products" not in decoded_response.get("data") or len(decoded_response.get('data').get('products')) == 0:
            return None

        if not response.request.url:
            return None
        
        parsed_url = urllib.parse.urlparse(response.request.url)
        page_number_str = urllib.parse.parse_qs(parsed_url.query)['page_number'][0]
        page_number = int(page_number_str)
        return {
            "page_number": page_number + 1
        }

class ProductDetails(TiktokShopStream):
    http_method = "GET"
    primary_key = "id"
    response_list_field = "products"

    def __init__(self, **kwargs):
        super().__init__(config = kwargs.get('config'))
        parent = kwargs.get('parent')
        self.parent = parent

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None;
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/products/details"

    def stream_slices(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_slices = self.parent.stream_slices(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state
        )

        # iterate over parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(
                sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )

            product_ids = []
            # iterate over all parent records with current stream_slice
            for record in parent_records:
                product_ids.append(record.get("id"))
            
            for id in product_ids:
                yield  {
                    "id": id
                }   
    def get_init_request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None):
        id = stream_slice.get("id")
        return {
            "app_key": self.app_key,
            "access_token": self.access_token,
            "product_id": id
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        if data["code"]:    
            raise TiktokException(data)
        yield data["data"]
        
class CategoryList(TiktokShopStream):
    http_method = "GET"
    primary_key = "id"
    response_list_field = "category_list"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/products/categories"

class AttributeList(TiktokShopStream):
    http_method = "GET"
    primary_key = "id"
    response_list_field = "attributes"

    def __init__(self, **kwargs):
        super().__init__(config = kwargs.get('config'))
        parent = kwargs.get('parent')
        self.parent = parent

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/products/attributes"
    
    def get_init_request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None):
        category_id = stream_slice.get("category_id")
        
        return {
            "app_key": self.app_key,
            "access_token": self.access_token,
            "category_id": category_id
        }

    def stream_slices(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_slices = self.parent.stream_slices(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state
        )

        # iterate over parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(
                sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )

            # iterate over all parent records with current stream_slice
            for record in parent_records:
                if record.get('is_leaf'):
                    yield  {
                        "category_id": record.get("id")
                    }   

class BrandList(TiktokShopStream):
    http_method = "GET"
    primary_key = "id"
    response_list_field = "brand_list"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/api/products/brands"