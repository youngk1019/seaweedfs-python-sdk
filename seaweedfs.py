import http
import json
import os

import requests

MB_SIZE = 1024 * 1024


class Client:
    def __init__(self, filer):
        self._url = "http://" + filer

    def list_object(self, prefix):
        headers = {
            "Accept": "application/json"
        }
        if prefix.startswith("/"):
            prefix = prefix[1:]

        ret = []
        load_more = True
        last_file_name = ""
        while load_more:
            params = {
                "lastFileName": last_file_name,
            }
            response = requests.get(
                url=self._url + "/" + prefix,
                headers=headers,
                params=params,
            )
            try:
                content = json.loads(response.content)
                for u in content["Entries"]:
                    ret.append(u["FullPath"])
                load_more = content["ShouldDisplayLoadMore"]
                last_file_name = content["LastFileName"]
            except:
                return ret
        return ret

    def put_object(self, src, dst,
                   datacenter="DefaultDataCenter",
                   rack="DefaultRack",
                   replication="",
                   ttl=""):
        if dst.startswith("/"):
            dst = dst[1:]
        if src == "":
            response = requests.post(
                url=self._url + "/" + dst,
                data=None,
            )
            return response
        params = {
            "maxMB": str(os.stat(src).st_size // MB_SIZE + 1),
            "dataCenter": datacenter,
            "rack": rack,
            "replication": replication,
            "ttl": ttl,
        }
        response = requests.post(
            url=self._url + "/" + dst,
            params=params,
            files={'file': open(src, "rb")},
        )
        return response

    def put_objects(self, src, dst,
                    recursive=True,
                    datacenter="DefaultDataCenter",
                    rack="DefaultRack",
                    replication="",
                    ttl=""):
        if not dst.endswith("/"):
            dst = dst + "/"
        response = requests.Response
        response.status_code = http.HTTPStatus.CREATED
        for f in os.listdir(src):
            src_path = os.path.join(src, f)
            dst_path = dst + f
            if os.path.isdir(src_path):
                if recursive:
                    response = self.put_objects(
                        src=src_path,
                        dst=dst_path,
                        recursive=recursive,
                        datacenter=datacenter,
                        rack=rack,
                        replication=replication,
                        ttl=ttl,
                    )
                    if response.status_code != http.HTTPStatus.CREATED:
                        return response
            else:
                response = self.put_object(
                    src=src_path,
                    dst=dst_path,
                    datacenter=datacenter,
                    rack=rack,
                    replication=replication,
                    ttl=ttl,
                )
                if response.status_code != http.HTTPStatus.CREATED:
                    return response
        return response

    def get_object(self, src):
        if src.startswith("/"):
            src = src[1:]
        response = requests.get(
            url=self._url + "/" + src,
        )
        return response.content

    def delete_object(self, src,
                      recursive=False,
                      ignore_recursive_error=False):
        params = {
            "recursive": "true" if recursive else "false",
            "ignoreRecursiveError": "true" if ignore_recursive_error else "false",
            "skipChunkDeletion": "false",
        }
        if src.startswith("/"):
            src = src[1:]
        response = requests.delete(
            url=self._url + "/" + src,
            params=params,
        )
        return response