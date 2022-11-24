# spring-cloud-alibaba-dubbo-issues-1595 (解决一些项目中遇到的问题)


#### 项目介绍（总共就4个Java文件）

分享一些作者在用spring-cloud-alibaba-dubbo时项目中遇到的问题，
通过将下面文件，路径原封不动复制到项目里，以达到覆盖源码的方式解决。

**1. 重写AbstractSpringCloudRegistry.class以解决获得支持**
    
      1.服务挂不上No provider
      2.一直掉老IP/端口 
      3.允许提供者与消费者无序启动 
      4.允许 -kill 9


**2. 重写DubboGenericServiceFactory.class以解决获得支持**
    
      1.解决接入其他注册中心(例：zk)的dubbo接口时，接口元信息可能会掉到其他注册中心上
      2.解决一直掉老IP/端口


**3. 重写ReferenceAnnotationBeanPostProcessor.class以解决获得支持**
    
      1.解决升级dubbo-2.7.15后，自定义注解用不了


**4. 重写NetUtils.class以解决获得支持**

      1.解决开VPN时, 有时获取VPN的IP，有时获取正常IP，导致服务掉不通。 
      2.解决开发环境多网卡时，IP总随机。
