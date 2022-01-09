import http
import json
import os
import sys
from io import BytesIO
from typing import BinaryIO

import requests

MB_SIZE = 1024 * 1024
Mode_Dir = 2 ** 31


class Client:
    def __init__(self, filer: str):
        self._url = "http://" + filer

    def list_object(self, prefix: str,
                    show_isdir=False,
                    ) -> list:
        headers = {
            "Accept": "application/json"
        }
        if not prefix.startswith("/"):
            prefix = "/" + prefix
        ret = []
        load_more = True
        last_file_name = ""
        while load_more:
            params = {
                "lastFileName": last_file_name,
            }
            try:
                response = requests.get(
                    url=self._url + prefix,
                    headers=headers,
                    params=params,
                )
                if response.status_code != http.HTTPStatus.OK:
                    print("Failed to list", prefix, response, file=sys.stderr)
                    return ret
            except requests.RequestException as e:
                print("Failed to list", prefix, e, file=sys.stderr)
                return ret
            try:
                try:
                    content = json.loads(response.content)
                except json.decoder.JSONDecodeError:
                    return ret
                for u in content["Entries"]:
                    if show_isdir:
                        ret.append({
                            "FullPath": u["FullPath"],
                            "isDir": u["Mode"] & Mode_Dir > 0,
                        })
                    else:
                        ret.append(u["FullPath"])
                load_more = content["ShouldDisplayLoadMore"]
                last_file_name = content["LastFileName"]
            except KeyError:
                return ret
        return ret

    def put_object(self, src: str,
                   dst: str,
                   datacenter="DefaultDataCenter",
                   rack="DefaultRack",
                   datanode="",
                   replication="",
                   ttl="",
                   debug=False,
                   ) -> requests.Response:
        if not dst.startswith("/"):
            dst = "/" + dst
        if src == "":
            response = requests.post(
                url=self._url + dst,
                data=None,
            )
            return response
        params = {
            "maxMB": str(os.stat(src).st_size // MB_SIZE + 1),
            "dataCenter": datacenter,
            "rack": rack,
            "dataNode": datanode,
            "replication": replication,
            "ttl": ttl,
        }
        try:
            response = requests.post(
                url=self._url + dst,
                params=params,
                files={'file': open(src, "rb")},
            )
            if response.status_code != http.HTTPStatus.CREATED:
                print("Failed to put", dst, response, file=sys.stderr)
                return response
            if debug:
                print("put", dst, "successfully", file=sys.stderr)
            return response
        except requests.RequestException as e:
            print("Failed to put", dst, e, file=sys.stderr)
            return e.response

    def put_objects(self, src: str,
                    dst: str,
                    recursive=True,
                    cover=False,
                    datacenter="DefaultDataCenter",
                    rack="DefaultRack",
                    datanode="",
                    replication="",
                    ttl="",
                    debug=False,
                    ) -> requests.Response:
        if not dst.endswith("/"):
            dst = dst + "/"
        if not dst.startswith("/"):
            dst = "/" + dst
        response = requests.Response
        response.status_code = http.HTTPStatus.CREATED
        exist = []
        if not cover:
            exist = self.list_object(dst)
        for f in os.listdir(src):
            src_path = os.path.join(src, f)
            dst_path = dst + f
            if os.path.isdir(src_path):
                if recursive:
                    response = self.put_objects(
                        src=src_path,
                        dst=dst_path,
                        recursive=recursive,
                        cover=cover,
                        datacenter=datacenter,
                        rack=rack,
                        datanode=datanode,
                        replication=replication,
                        ttl=ttl,
                        debug=debug,
                    )
                    if response.status_code != http.HTTPStatus.CREATED:
                        return response
            else:
                if dst_path in exist:
                    continue
                response = self.put_object(
                    src=src_path,
                    dst=dst_path,
                    datacenter=datacenter,
                    rack=rack,
                    datanode=datanode,
                    replication=replication,
                    ttl=ttl,
                    debug=debug,
                )
                if response.status_code != http.HTTPStatus.CREATED:
                    return response
        return response

    def get_object(self, src: str,
                   debug=False,
                   ) -> BinaryIO:
        if not src.startswith("/"):
            src = "/" + src
        try:
            response = requests.get(
                url=self._url + src,
            )
            if response.status_code != http.HTTPStatus.OK:
                print("Failed to get", src, response, file=sys.stderr)
                return BytesIO(b"")
            if debug:
                print("get", src, "successfully", file=sys.stderr)
            return BytesIO(response.content)
        except requests.RequestException as e:
            print("Failed to get", src, e, file=sys.stderr)
            return BytesIO(b"")

    def delete_object(self, src: str,
                      recursive=False,
                      ignore_recursive_error=False,
                      ) -> requests.Response:
        params = {
            "recursive": "true" if recursive else "false",
            "ignoreRecursiveError": "true" if ignore_recursive_error else "false",
            "skipChunkDeletion": "false",
        }
        if not src.startswith("/"):
            src = "/" + src
        try:
            response = requests.delete(
                url=self._url + src,
                params=params,
            )
            if response.status_code != http.HTTPStatus.NO_CONTENT:
                print("Failed to delete", src, response, file=sys.stderr)
            return response
        except requests.RequestException as e:
            print("Failed to delete", src, e, file=sys.stderr)
            return e.response

    def is_dir(self, src: str) -> bool:
        if not src.startswith("/"):
            src = "/" + src
        try:
            response = requests.head(
                url=self._url + src,
            )
            if response.status_code == http.HTTPStatus.NOT_FOUND:
                print(src, "is not exist", file=sys.stderr)
                return False
            if response.status_code != http.HTTPStatus.OK:
                print("Failed to is dir", src, response, file=sys.stderr)
                return False
            headers = response.headers
            if "Content-Length" in headers:
                return False
            if "Content-Type" in headers:
                return True
            print(src, "is not exist", file=sys.stderr)
            return False
        except requests.RequestException as e:
            print("Failed to is dir", src, e.response, file=sys.stderr)
            return True
