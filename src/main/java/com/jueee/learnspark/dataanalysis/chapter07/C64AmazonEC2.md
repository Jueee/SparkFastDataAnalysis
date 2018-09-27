### 7.6.4　Amazon EC2 ###
Spark 自带一个可以在 Amazon EC2 上启动集群的脚本。  
这个脚本会启动一些节点，并且在它们上面安装独立集群管理器。  
EC2 脚本还会安装好其他相关的服务，比如HDFS、Tachyon 还有用来监控集群的 Ganglia。

Spark 的 EC2 脚本叫作 spark-ec2 ，位于 Spark 安装目录下的 ec2 文件夹中。  

#### 启动集群 ####
先创建一个 Amazon 网络服务（AWS）账号，并且获取访问键 ID 和访问键密码，然后把它们设在环境变量中：
``` 
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```
然后再创建出 EC2 的 SSH 密钥对，然后下载私钥文件（通常叫作 keypair.pem），这样你就可以 SSH 到你的机器上。  

#### 登录集群 ####
可以使用存有私钥的 .pem 文件通过 SSH 登录到集群的主节点上。  

登录命令：
``` 
./spark-ec2 -k mykeypair -i mykeypair.pem login mycluster
```

#### 销毁集群 ####
```
./spark-ec2 destroy mycluster
```

#### 中止集群 ####
```
./spark-ec2 stop mycluster
```

#### 再次启动集群 ####
```
./spark-ec2 -k mykeypair -i mykeypair.pem start mycluster
```