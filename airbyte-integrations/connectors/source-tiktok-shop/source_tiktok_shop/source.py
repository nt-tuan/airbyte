#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
import urllib.parse
import hmac
import hashlib
import base64
import time

from .streams import (
    ActiveShops,
    AttributeList,
    BrandList,
    CategoryList,
    OrderList,
    OrderDetails,
    ProductDetails,
    ProductList
)

class TiktokException(Exception):
    """default exception for custom Tiktok logic"""

# Source
class SourceTiktokShop(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # return [ActiveShops(authenticator=auth), Employees(authenticator=auth)]
        # should generate access_token here
        
        order_stream = OrderList(config = config)
        order_details_stream = OrderDetails(parent = order_stream,config = config)
        product_stream = ProductList(config)
        product_details_stream = ProductDetails(parent = product_stream, config = config)
        category_stream = CategoryList(config)
        attribute_stream = AttributeList(parent = category_stream, config = config)
        brand_stream = BrandList(config)


        return [
            order_stream, 
            order_details_stream, 
            product_stream,
            product_details_stream,
            category_stream,
            attribute_stream,
            brand_stream
        ]
    