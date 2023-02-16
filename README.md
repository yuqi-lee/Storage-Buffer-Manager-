# Storage-Buffer-Manager

中国科大 2022 秋季学期《高级数据库》课程实验代码仓库

## 目录说明

### src

### doc

### 其它文件


## 使用方法

### go语言版本
运行程序需要升级`go`版本至`1.18`及以上————`Buffer Manager`的`lru cache`使用了第三方的双向链表库，需要支持go泛型


### 编译并运行

进入src目录并编译

```sh
cd src && go build .
```

运行可执行文件（以Windows为例）

```sh
./adbslab.com.exe
```

> 根据需要修改`main.go`中benchmark文件路径
> dbf文件会自动生成
