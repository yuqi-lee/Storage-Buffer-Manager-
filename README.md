# Storage-Buffer-Manager

2022Fall USTC-Advanced-Database-System Lab

## 目录说明

### src

### doc

### 其它文件


## 使用方法

### go语言版本
运行程序需要升级go版本至1.18及以上————`Buffer Manager`的`lru cache`使用了第三方的双向链表库，需要支持go泛型


### 编译并运行

进入src目录并编译

```sh
cd src && go build .
```

运行可执行文件（以Windows为例）

```sh
./adbslab.com.exe
```

> 根据需要修改测试benchmark文件路径
> dbf文件会自动生成
