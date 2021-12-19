from seaweedfs import Client


def main():
    client = Client(
        # filer的ip:port
        filer="10.249.177.55:8888"
    )
    dir_path = "C:/Users/youngk/Desktop/test"
    # 这个文件夹test下包含了1-3.txt 和一个子文件夹test1 test1包含了4-6.txt
    file_path = "C:/Users/youngk/Desktop/100.txt"

    # 远端测试文件夹清空
    client.delete_object("test", recursive=True)

    # put_object
    # 上传文件 参数为 src=本地的目录地址 dst=保存到远端的目录地址 datacenter(可选)=优先的数据中心 rack(可选)=优先的机架
    # replication(可选)=备份机制,默认无 查看https://github.com/chrislusf/seaweedfs/wiki/Replication填写 ttl(可选，默认无过期时间)=过期时间
    # 返回一个requests.Response 上传文件保证不会分块(chunks)
    # 远端的目录地址前可以加上/也可以不加 test/1.txt 和 /test/1.txt 等效 后续同理
    # put_object的dst最后加上/则会自动命名为文件名,如果上传1.txt 到 test/ 远端则会命名为test/1.txt
    # 如果创建一个文件夹，则src填写""即可 dst最后必须加上/

    # 正常上传
    client.put_object(file_path, "test/98.txt")
    client.put_object(file_path, "/test/99.txt")
    # list_object
    # 展示某个目录下所有的文件和文件夹 返回一个list，为文件或文件架的完整路径 参数为prefix=查询的目录路径
    # 如果失败则返回一个空的list
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/98.txt', '/test/99.txt']

    # 自动补全名称
    client.put_object(file_path, "test/")
    obs = client.list_object("/test")
    print(obs)
    # 返回['/test/100.txt', '/test/98.txt', '/test/99.txt']

    # 创建文件夹
    client.put_object("", "test/test2/")
    obs = client.list_object("/test")
    print(obs)
    # 返回['/test/100.txt', '/test/98.txt', '/test/99.txt', '/test/test2']

    # put_objects
    # 上传多个文件 参数为 src=本地的目录地址,需为一个文件夹地址 dst=保存到远端的目录地址(为一个目录地址)
    # recursive(可选)=是否上传子文件的内容，默认自动上传子文件内的内容，关闭则填False datacenter(可选)=优先的数据中心 rack(可选)=优先的机架
    # replication(可选)=备份机制,默认无 查看https://github.com/chrislusf/seaweedfs/wiki/Replication填写 ttl(可选，默认无过期时间)=过期时间
    # 返回一个requests.Response 上传文件保证不会分块(chunks) 如果成功则返回的是最后一个上传文件的requests.Response 如果失败则返回失败呢个文件的requests.Response
    # 远端的目录地址后可以加上/也可以不加上 即test 和test/ 都认为是一个名为test的文件夹

    # 未上传子文件夹test1内容
    client.put_objects(dir_path, "test", recursive=False)
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/1.txt', '/test/100.txt', '/test/2.txt', '/test/3.txt', '/test/98.txt', '/test/99.txt', '/test/test2']

    # 上传子文件夹内容
    client.put_objects(dir_path, "test")
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/1.txt', '/test/100.txt', '/test/2.txt', '/test/3.txt', '/test/98.txt', '/test/99.txt', '/test/test1', '/test/test2']
    obs = client.list_object("test/test1")
    print(obs)
    # 返回['/test/test1/4.txt', '/test/test1/5.txt', '/test/test1/6.txt']

    # delete_object
    # 删除文件，参数为 src=保存到远端的完整目录路径
    # recursive(可选)=是否递归删除 默认不递归删除 如果删除文件夹，文件夹内包含文件，则无法删除，如果不包含文件则可以删除
    # ignore_recursive_error(可选)=忽略递归删除的错误 默认不忽略
    # 如果recursive=True 则包含文件也可以实现递归删除
    # 返回一个requests.Response

    # 删除单个文件
    client.delete_object("test/98.txt")
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/1.txt', '/test/100.txt', '/test/2.txt', '/test/3.txt', '/test/99.txt', '/test/test1', '/test/test2']

    # 删除空文件夹成功
    client.delete_object("test/test2")
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/1.txt', '/test/100.txt', '/test/2.txt', '/test/3.txt', '/test/99.txt', '/test/test1']

    # 删除存在文件的文件夹 非递归删除失败
    client.delete_object("test/test1")
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/1.txt', '/test/100.txt', '/test/2.txt', '/test/3.txt', '/test/99.txt', '/test/test1']

    # 递归删除文件夹成功
    client.delete_object("test/test1", recursive=True)
    obs = client.list_object("test")
    print(obs)
    # 返回['/test/1.txt', '/test/100.txt', '/test/2.txt', '/test/3.txt', '/test/99.txt']

    for ob in obs:
        # get_object
        # 下载文件 ，参数为 src=保存到远端的完整目录路径，返回一个bytes，为文件的二进制信息,如果下载失败则返回一个长度为零的bytes
        ret = client.get_object(ob)
        with open(ob[6:], "wb") as f:
            f.write(ret)


if __name__ == "__main__":
    main()
