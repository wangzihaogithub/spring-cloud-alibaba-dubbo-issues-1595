/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.cloud.dubbo.registry;

import com.alibaba.cloud.dubbo.metadata.repository.DubboServiceMetadataRepository;
import com.alibaba.cloud.dubbo.registry.event.ServiceInstancesChangedEvent;
import com.alibaba.cloud.dubbo.service.DubboGenericServiceFactory;
import com.alibaba.cloud.dubbo.service.DubboMetadataService;
import com.alibaba.cloud.dubbo.service.DubboMetadataServiceProxy;
import com.alibaba.cloud.dubbo.util.JSONUtils;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.rpc.cluster.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.dubbo.common.URLBuilder.from;
import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.ADMIN_PROTOCOL;
import static org.springframework.util.StringUtils.hasText;

/**
 * 解决新服务总挂不上的问题 - 王子豪
 *
 * Abstract Dubbo {@link RegistryFactory} uses Spring Cloud Service Registration
 * abstraction, whose protocol is "spring-cloud".
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 */
@Deprecated
public abstract class AbstractSpringCloudRegistry extends FailbackRegistry {

	/**
	 * The parameter name of {@link #servicesLookupInterval}.
	 */
	public static final String SERVICES_LOOKUP_INTERVAL_PARAM_NAME = "dubbo.services.lookup.interval";

	protected static final String DUBBO_METADATA_SERVICE_CLASS_NAME = DubboMetadataService.class
			.getName();

	/**
	 * Caches the IDs of {@link ApplicationListener}.
	 */
	private static final Map<String, ServiceInstancesChangedEventApplicationListener> REGISTER_LISTENERS = new LinkedHashMap<>();
	private static final Map<String, ServiceInstancesChangedEventApplicationListener> REGISTER_LISTENERS_CONCURRENT = new ConcurrentHashMap<>();

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * The interval in second of lookup service names(only for Dubbo-OPS).
	 */
	private final long servicesLookupInterval;

	private final DiscoveryClient discoveryClient;

	private final DubboServiceMetadataRepository repository;

	private final DubboMetadataServiceProxy dubboMetadataConfigServiceProxy;

	private final JSONUtils jsonUtils;

	private final DubboGenericServiceFactory dubboGenericServiceFactory;

	private final ConfigurableApplicationContext applicationContext;
	private final String currentApplicationName;
	private static final List<AbstractSpringCloudRegistry> REGISTRY_LIST = new ArrayList<>();
	private volatile boolean startupSubscribeDubboServiceURL = false;
	private volatile boolean acceptingTraffic = false;

	public AbstractSpringCloudRegistry(URL url, DiscoveryClient discoveryClient,
									   DubboServiceMetadataRepository dubboServiceMetadataRepository,
									   DubboMetadataServiceProxy dubboMetadataConfigServiceProxy,
									   JSONUtils jsonUtils, DubboGenericServiceFactory dubboGenericServiceFactory,
									   ConfigurableApplicationContext applicationContext) {
		// 禁用文件缓存（如果单机部署，可能会访问同一个文件） - 王子豪
		super(url.addParameter("file.cache","false"));
		this.servicesLookupInterval = url
				.getParameter(SERVICES_LOOKUP_INTERVAL_PARAM_NAME, 60L);
		this.discoveryClient = discoveryClient;
		this.repository = dubboServiceMetadataRepository;
		this.dubboMetadataConfigServiceProxy = dubboMetadataConfigServiceProxy;
		this.jsonUtils = jsonUtils;
		this.dubboGenericServiceFactory = dubboGenericServiceFactory;
		this.applicationContext = applicationContext;
		this.currentApplicationName = applicationContext.getEnvironment().getProperty("spring.application.name", "");

		// 应用启动后更新 - 王子豪
		applicationContext.addApplicationListener(new ApplicationListener<AvailabilityChangeEvent>() {
			@Override
			public void onApplicationEvent(AvailabilityChangeEvent event) {
				if (event.getState() != ReadinessState.ACCEPTING_TRAFFIC) {
					return;
				}
				Set<String> services = repository.getSubscribedServices();
				startupSubscribeDubboServiceURL = true;
				try {
					logger.info("startup ACCEPTING_TRAFFIC [{}], REGISTER_LISTENERS_SIZE = [{}], REGISTER_LISTENERS = [{}]",
							services, REGISTER_LISTENERS.size(), new ArrayList<>(REGISTER_LISTENERS.keySet()));
					for (String service : services) {
						// 跳过自己
						if (currentApplicationName.equals(service)) {
							continue;
						}
						List<ServiceInstance> instanceList = discoveryClient.getInstances(service);
						if (instanceList != null && instanceList.size() > 0) {
							for (ServiceInstancesChangedEventApplicationListener listener : REGISTER_LISTENERS.values()) {
								subscribeDubboServiceURL(listener.url, listener.listener, service,
										s -> instanceList);
							}

							while (!REGISTER_LISTENERS_CONCURRENT.isEmpty()) {
								REGISTER_LISTENERS.putAll(REGISTER_LISTENERS_CONCURRENT);
								ArrayList<ServiceInstancesChangedEventApplicationListener> snapshot = new ArrayList<>(REGISTER_LISTENERS_CONCURRENT.values());
								REGISTER_LISTENERS_CONCURRENT.clear();
								for (ServiceInstancesChangedEventApplicationListener listener : snapshot) {
									subscribeDubboServiceURL(listener.url, listener.listener, service,
											s -> instanceList);
								}
							}
						}
					}
				}finally {
					startupSubscribeDubboServiceURL = false;
					acceptingTraffic = true;
					REGISTER_LISTENERS.putAll(REGISTER_LISTENERS_CONCURRENT);
					REGISTER_LISTENERS_CONCURRENT.clear();
				}
			}
		});
		REGISTRY_LIST.add(this);
	}

	/**
	 * 主动更新服务地址 - 王子豪
	 * @param directory
	 */
	public static void update(Directory directory){
		try {
			if(directory == null || directory.getConsumerUrl() == null){
				return;
			}
			if (!"spring-cloud".equals(directory.getConsumerUrl().getProtocol())) {
				return;
			}
		}catch (Exception ignored){
			return;
		}

		for (AbstractSpringCloudRegistry registry : REGISTRY_LIST) {
			if(!registry.acceptingTraffic){
				continue;
			}
			DiscoveryClient discoveryClient = registry.discoveryClient;
			Set<String> services = registry.repository.getSubscribedServices();
			for (String service : services) {
				// 跳过自己
				if (registry.currentApplicationName.equals(service)) {
					continue;
				}
				if(registry.isDubboMetadataServiceURL(directory.getConsumerUrl())){
					continue;
				}
				if(directory instanceof NotifyListener){
					List<ServiceInstance> instances = discoveryClient.getInstances(service);
					List<URL> allSubscribedURLs = registry.getAllSubscribedURLs(directory.getConsumerUrl(), instances);
					((NotifyListener) directory).notify(allSubscribedURLs);
				}
			}
		}
	}

	protected boolean shouldRegister(URL url) {
		String side = url.getParameter(SIDE_KEY);

		boolean should = PROVIDER_SIDE.equals(side); // Only register the Provider.

		if (!should) {
			if (logger.isDebugEnabled()) {
				logger.debug("The URL[{}] should not be registered.", url.toString());
			}
		}

		return should;
	}

	@Override
	public final void doRegister(URL url) {
		if (!shouldRegister(url)) {
			return;
		}
		doRegister0(url);
	}

	/**
	 * The sub-type should implement to register.
	 * @param url {@link URL}
	 */
	protected abstract void doRegister0(URL url);

	@Override
	public final void doUnregister(URL url) {
		if (!shouldRegister(url)) {
			return;
		}
		doUnregister0(url);
	}

	/**
	 * The sub-type should implement to unregister.
	 * @param url {@link URL}
	 */
	protected abstract void doUnregister0(URL url);

	@Override
	public final void doSubscribe(URL url, NotifyListener listener) {

		if (isAdminURL(url)) {
			// TODO in future
		}
		else if (isDubboMetadataServiceURL(url)) { // for DubboMetadataService
			subscribeDubboMetadataServiceURLs(url, listener);
			if (from(url).getParameter(CATEGORY_KEY) != null
					&& from(url).getParameter(CATEGORY_KEY).contains(PROVIDER)) {
				// Fix #1259 and #753 Listene meta service change events to remove useless
				// clients
				registerServiceInstancesChangedEventListener(url, listener);
			}

		}
		else { // for general Dubbo Services
			subscribeDubboServiceURLs(url, listener);
		}
	}

	protected void subscribeDubboServiceURLs(URL url, NotifyListener listener) {
		/**
		 * 启动中不更新，启动中只更新元数据，服务延迟到启动后更新 - 王子豪
		 */
		Set<String> subscribedServices = repository.getSubscribedServices();
		for (String subscribedService : subscribedServices) {
			initializeMetadata(url, subscribedService);
		}
		registerServiceInstancesChangedEventListener(url, listener);
	}

	/**
	 * Register a {@link ApplicationListener listener} for
	 * {@link ServiceInstancesChangedEvent}.
	 * @param url {@link URL}
	 * @param listener {@link NotifyListener}
	 */
	private void registerServiceInstancesChangedEventListener(URL url,
															  NotifyListener listener) {
		if (isDubboMetadataServiceURL(url)) {
			return;
		}
		/**
		 * 每次服务更新后，删除旧提供者地址，以防提供者IP或端口变化 - 王子豪
		 */
		if(REGISTER_LISTENERS.isEmpty()){
			applicationContext.addApplicationListener(new ApplicationListener<ServiceInstancesChangedEvent>(){
				@Override
				public void onApplicationEvent(ServiceInstancesChangedEvent event) {
					/**
					 * 每次服务更新后，删除旧提供者地址，以防提供者IP或端口变化 - 王子豪
					 */
					dubboMetadataConfigServiceProxy.removeProxy(event.getServiceName());
				}
			});
		}

		/**
		 * 增加 identityHashCode，避免出现有listener没注册上 - 王子豪
		 */
		String listenerId = generateId(url) + "-" + System.identityHashCode(listener);
		ServiceInstancesChangedEventApplicationListener applicationListener =
				new ServiceInstancesChangedEventApplicationListener(this, url, listener);
		if(startupSubscribeDubboServiceURL){
			/**
			 * 解决执行时序导致漏接口没监听上 - 王子豪
			 */
			REGISTER_LISTENERS_CONCURRENT.put(listenerId, applicationListener);
		} else if (REGISTER_LISTENERS.put(listenerId, applicationListener) == null) {
			applicationContext.addApplicationListener(applicationListener);
		}
	}

	/**
	 * 变量存下来，支持启动后再重放监听事件 - 王子豪
	 */
	public static class ServiceInstancesChangedEventApplicationListener	implements ApplicationListener<ServiceInstancesChangedEvent> {
		private AbstractSpringCloudRegistry registry;
		private URL url;
		private NotifyListener listener;
		private final Thread thread = Thread.currentThread();

		public ServiceInstancesChangedEventApplicationListener(AbstractSpringCloudRegistry registry, URL url, NotifyListener listener) {
			this.registry = registry;
			this.url = url;
			this.listener = listener;
		}

		@Override
		public void onApplicationEvent(
				ServiceInstancesChangedEvent event) {
			String serviceName = event.getServiceName();
			Collection<ServiceInstance> serviceInstances = event
					.getServiceInstances();
			if(registry.acceptingTraffic){
				registry.resubscribeDubboServiceURL(url, listener, serviceName,
						s -> serviceInstances);
			}
		}
	}

	private void initializeMetadata(URL url,String serviceName) {
		if (isDubboMetadataServiceURL(url)) {
			// Prevent duplicate generation of DubboMetadataService
			return;
		}
		repository.initializeMetadata(serviceName);
	}

	protected void subscribeDubboServiceURL(URL url, NotifyListener listener,
											String serviceName,
											Function<String, Collection<ServiceInstance>> serviceInstancesFunction) {

		if (logger.isDebugEnabled()) {
			logger.debug(
					"The Dubbo Service URL[ID : {}] is being subscribed for service[name : {}]",
					generateId(url), serviceName);
		}

		Collection<ServiceInstance> serviceInstances = serviceInstancesFunction
				.apply(serviceName);

		// issue : ReStarting a consumer and then starting a provider does not
		// automatically discover the registration
		// fix https://github.com/alibaba/spring-cloud-alibaba/issues/753
		// Re-obtain the latest list of available metadata address here, ip or port may
		// change.
		// by https://github.com/wangzihaogithub
		// When the last service provider is closed, 【fix 1259】while close the
		// channel，when up a new provider then repository.initializeMetadata(serviceName)
		// will throw Exception.
		// dubboMetadataConfigServiceProxy.removeProxy(serviceName);
		// repository.removeMetadataAndInitializedService(serviceName);
		// dubboGenericServiceFactory.destroy(serviceName);
		// repository.initializeMetadata(serviceName);
		if (CollectionUtils.isEmpty(serviceInstances)) {
			/**
			 * 删掉无作用代码 - 王子豪
			 */
//			if (logger.isWarnEnabled()) {
//				logger.warn(
//						"There is no instance from service[name : {}], and then Dubbo Service[key : {}] will not be "
//								+ "available , please make sure the further impact",
//						serviceName, url.getServiceKey());
//			}
			return;
			/**
			 * 删掉无作用代码 - 王子豪
			 */
//			if (isDubboMetadataServiceURL(url)) {
//				// if meta service change, and serviceInstances is zero, will clean up
//				// information about this client
//				dubboMetadataConfigServiceProxy.removeProxy(serviceName);
//				repository.removeMetadataAndInitializedService(serviceName, url);
//				dubboGenericServiceFactory.destroy(serviceName);
//				String listenerId = generateId(url);
//				// The metaservice will restart the new listener. It needs to be optimized
//				// to see whether the original listener can be reused.
//				REGISTER_LISTENERS.remove(listenerId);
//			}
//
//			/**
//			 * URLs with {@link RegistryConstants#EMPTY_PROTOCOL}
//			 */
//			allSubscribedURLs.addAll(emptyURLs(url));
//			if (logger.isDebugEnabled()) {
//				logger.debug("The subscribed URL[{}] will notify all URLs : {}", url,
//						allSubscribedURLs);
//			}
//			listener.notify(allSubscribedURLs);
//			return;
		}
		if (isDubboMetadataServiceURL(url)) {
			// Prevent duplicate generation of DubboMetadataService
			return;
		}
		repository.initializeMetadata(serviceName);
		List<URL> allSubscribedURLs = getAllSubscribedURLs(url, serviceInstances);
		if(allSubscribedURLs == null || allSubscribedURLs.isEmpty()){
			/**
			 * 不推空服务地址 - 王子豪
			 */
			if (logger.isDebugEnabled()) {
				logger.debug("The subscribed URL[{}] will notify all URLs : {}", url,
						allSubscribedURLs);
			}
			return;
		}
		listener.notify(allSubscribedURLs);
	}

	/**
	 * 将重建逻辑与初始化逻辑区分开来 - 王子豪
	 */
	private void resubscribeDubboServiceURL(URL url, NotifyListener listener,
											String serviceName,
											Function<String, Collection<ServiceInstance>> serviceInstancesFunction) {
		if (isDubboMetadataServiceURL(url)) {
			return;
		}
		Collection<ServiceInstance> serviceInstances = serviceInstancesFunction.apply(serviceName);
		if (CollectionUtils.isEmpty(serviceInstances)) {
			logger.debug("The resubscribed serviceName[{}] serviceInstances is empty skip URL[{}] ",
					serviceName,url);
			return;
		}
		List<URL> allSubscribedURLs = getAllSubscribedURLs(url, serviceInstances);
		if(allSubscribedURLs == null || allSubscribedURLs.isEmpty()){
			logger.warn("The resubscribed serviceName[{}] allSubscribedURLs is empty serviceInstances[{}] URL[{}]",
					serviceName,
					serviceInstances,
					url);
		}else {
//			logger.info("The resubscribed serviceName[{}] serviceInstances[{}] URL[{}] will notify all URLs : {}",
//					serviceName,serviceInstances,url,allSubscribedURLs);
			listener.notify(allSubscribedURLs);
		}
	}

	/**
	 * 这段逻辑要复用 - 王子豪
	 * @param url consumer://192.168.3.23/com.ig.service.api.hunter.PUserService?application=zhipin-quake-web&async=false&category=providers,configurators,routers&check=false&dubbo=2.0.2&init=false&interface=com.ig.service.api.hunter.PUserService&lazy=true&methods=selectPUserList,getAuthDeptIdListByUserId&pid=8498&qos.enable=false&reconnect=true&reference.filter=apacheTracing,apacheTracing&release=2.7.18&retries=-1&revision=2.0.0&send.reconnect=true&sent=false&side=consumer&sticky=false&timeout=10000&timestamp=1669263504284&version=2.0.0
	 * @param serviceInstances
	 * @return
	 */
	private List<URL> getAllSubscribedURLs(URL url,Collection<ServiceInstance> serviceInstances){
		List<URL> allSubscribedURLs = new LinkedList<>();
		DubboMetadataService dubboMetadataService = dubboMetadataConfigServiceProxy
				.getProxy(new ArrayList<>(serviceInstances));
		if (dubboMetadataService == null) { // It makes sure not-found, return immediately
			if (logger.isWarnEnabled()) {
				logger.warn(
						"The metadata of Dubbo service[key : {}] still can't be found, it could effect the further "
								+ "Dubbo service invocation",
						url.getServiceKey());
			}
			return null;
		}

		List<URL> exportedURLs = getExportedURLs(dubboMetadataService, url);
		for (URL exportedURL : exportedURLs) {
			String protocol = exportedURL.getProtocol();
			List<URL> subscribedURLs = new LinkedList<>();
			serviceInstances.forEach(serviceInstance -> {
				Integer port = repository.getDubboProtocolPort(serviceInstance, protocol);
				String host = serviceInstance.getHost();
				if (port == null) {
					if (logger.isWarnEnabled()) {
						logger.warn(
								"The protocol[{}] port of Dubbo  service instance[host : {}] "
										+ "can't be resolved",
								protocol, host);
					}
				}
				else {
					URL subscribedURL = new URL(protocol, host, port,
							exportedURL.getParameters());
					subscribedURLs.add(subscribedURL);
				}
			});

			allSubscribedURLs.addAll(subscribedURLs);
		}
		return allSubscribedURLs;
	}

	private String generateId(URL url) {
		return url.toString(VERSION_KEY, GROUP_KEY, PROTOCOL_KEY);
	}

	private List<URL> emptyURLs(URL url) {
		// issue : When the last service provider is closed, the client still periodically
		// connects to the last provider.n
		// fix https://github.com/alibaba/spring-cloud-alibaba/issues/1259
		return asList(from(url).setProtocol(EMPTY_PROTOCOL).removeParameter(CATEGORY_KEY)
				.build());
	}

	private List<ServiceInstance> getServiceInstances(String serviceName) {
		return hasText(serviceName) ? doGetServiceInstances(serviceName) : emptyList();
	}

	private List<ServiceInstance> doGetServiceInstances(String serviceName) {
		List<ServiceInstance> serviceInstances = emptyList();
		try {
			serviceInstances = discoveryClient.getInstances(serviceName);
		}
		catch (Exception e) {
			if (logger.isErrorEnabled()) {
				logger.error(e.getMessage(), e);
			}
		}
		return serviceInstances;
	}

	private List<URL> getExportedURLs(DubboMetadataService dubboMetadataService,
									  URL url) {
		String serviceInterface = url.getServiceInterface();
		String group = url.getParameter(GROUP_KEY);
		String version = url.getParameter(VERSION_KEY);
		// The subscribed protocol may be null
		String subscribedProtocol = url.getParameter(PROTOCOL_KEY);
		String exportedURLsJSON = dubboMetadataService.getExportedURLs(serviceInterface,
				group, version);
		return jsonUtils.toURLs(exportedURLsJSON).stream()
				.filter(exportedURL -> subscribedProtocol == null
						|| subscribedProtocol.equalsIgnoreCase(exportedURL.getProtocol()))
				.collect(Collectors.toList());
	}

	private void subscribeDubboMetadataServiceURLs(URL url, NotifyListener listener) {
		List<URL> urls = repository.findSubscribedDubboMetadataServiceURLs(url);
		listener.notify(urls);
	}

	@Override
	public final void doUnsubscribe(URL url, NotifyListener listener) {
		// if (isAdminURL(url)) {
		// }
	}

	@Override
	public boolean isAvailable() {
		return !discoveryClient.getServices().isEmpty();
	}

	protected boolean isAdminURL(URL url) {
		return ADMIN_PROTOCOL.equals(url.getProtocol());
	}

	protected boolean isDubboMetadataServiceURL(URL url) {
		return DUBBO_METADATA_SERVICE_CLASS_NAME.equals(url.getServiceInterface());
	}

}
