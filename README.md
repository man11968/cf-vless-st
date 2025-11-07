 -------------------------------------------------------------
### 一、Cloudflare部署代理脚本js源码

1. 本项目收集了一些CF部署原码并通过本人理解并精心修改重构，旨在开源以供他人所需
2. 本项目仅支持本地化CF部署，强烈建议纯手搓节点，后面有手搓节点示意图
3. 打开源码：_worker.js，部署前请认真阅读代码头部的注释"使用说明"
 -------------------------------------------------------------
### 二、脚本特色
#### (一) 支持workers、pages、snippets部署，vless+ws+tls代理节点
#### (二) 极大的丰富了反代功能的使用
v2rayN客户端的单节点路径设置代理ip，通过代理客户端路径传递<br>
支持IPV4、IPV6、域名三种方式（端口为443时，可不写:端口）,以下任选其一<br>
| 代理类型 | IPv4形式| IPv6形式| 域名形式|
| :--- | :--- | :--- | :--- |
| 全局代理 |s5all=IPV4地址:端口|s5all=[IPV6地址]:端口 |s5all=域名:端口|
| 全局代理 |httpall=IPV4地址:端口|httpall=[IPV6地址]:端口|httpall=域名:端口|
| 代理cf相关网站 |http=IPV4地址:端口|http=[IPV6地址]:端口|http=域名:端口|
| 代理cf相关网站 |http://IPV4地址:端口|http://[IPV6地址]:端口|http://域名:端口|
| 代理cf相关网站 |socks5=IPV4地址:端口|socks5=[IPV6地址]:端口|socks5=域名:端口|
| 代理cf相关网站 |socks5://IPV4地址:端口|socks5://[IPV6地址]:端口|socks5://域名:端口|
| 代理cf相关网站 |pyip=IPV4地址:端口|pyip=[IPV6地址]:端口|pyip=域名:端口|
| 代理cf相关网站 |proxyip=IPV4地址:端口|proxyip=[IPV6地址]:端口|proxyip=域名:端口|
| 代理cf相关网站 | |nat64pf=[2602:fc59:b0:64::]| |
#### 注意
1. workers、pages、snippets都可以部署，纯手搓443系6个端口节点vless+ws+tls
2. snippets部署的，nat64及william的proxyip域名"不支持"
#### (三) 纯手搓示意图（以v2rayN客户端为例）
   ![这是图片](/image/手搓.png "vless")<br>
 -------------------------------------------------------------
### 三、优选IP的运用
1. CF官方优选80系端口：80、8080、8880、2052、2082、2086、2095
2. CF官方优选443系端口：443、2053、2083、2087、2096、8443 <br>
   如果你没有天天最高速度或者选择国家的需求，使用默认的CF官方IP或者域名即可，不必更换
3. 推荐下面是优选官方IP大段支持13个标准端口切换 
   ##### 104.16.0.0 ; 104.17.0.0 ; 104.18.0.0 ; 104.19.0.0 ; 104.20.0.0 ; 104.21.0.0 ; 104.22.0.0 ; 104.24.0.0 ; 104.25.0.0 ; 104.26.0.0 ; 104.27.0.0
   ##### 172.66.0.0 ; 172.67.0.0
   ##### 162.159.0.0
   ##### 2606:4700::0 需IPV6环境
 -------------------------------------------------------------
### 四、客户端推荐
#### 点击名称即跳转到官方下载地址
1. 安卓Android：[v2rayNG](https://github.com/2dust/v2rayNG/tags)、[Nekobox](https://github.com/starifly/NekoBoxForAndroid/releases)、[Karing](https://github.com/KaringX/karing/tags) <br>

2. 电脑Windows：[v2rayN](https://github.com/2dust/v2rayN/tags)、[Hiddify](https://github.com/hiddify/hiddify-next/tags)、[Karing](https://github.com/KaringX/karing/tags)
-------------------------------------------------------------
## 感谢您右上角加Star🌟
[![Star History Chart](https://api.star-history.com/svg?repos=duquancai/cf-vless-st&type=Date)](https://www.star-history.com/#duquancai/cf-vless-st&Date)