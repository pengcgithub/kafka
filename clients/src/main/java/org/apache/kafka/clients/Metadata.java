/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 * 
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    /**
     * 两次发出更新 Cluster 保存的元数据信息的最小时间差，默认为 100ms。防止更新操作过于频繁而造成网络阻塞和增加服务端压力。
     */
    private final long refreshBackoffMs;

    /**
     * 元数据的失效时间，也就是需要更新元数据的时间间隔，默认5分钟
     */
    private final long metadataExpireMs;
    private int version;

    /**
     * 最近一次尝试更新元数据的时间戳
     */
    private long lastRefreshMs;

    /**
     * 最近一次成功更新元数据的时间戳
     */
    private long lastSuccessfulRefreshMs;

    /**
     * 元数据数据结构
     */
    private Cluster cluster;
    private boolean needUpdate;
    /**
     * 客户端缓存的topic
     */
    private final Set<String> topics;
    /**
     * 缓存的监听器
     */
    private final List<Listener> listeners;
    /**
     * 是否强制更新所有的metadata
     */
    private boolean needMetadataForAllTopics;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashSet<String>();
        this.listeners = new ArrayList<>();
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata
     */
    public synchronized void add(String topic) {
        topics.add(topic);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * 就是将version传了进去，然后version不变的话就会一直循环，sender拉取到了元数据信息，然后更新本地的缓存的时候，会将这个version+1，这个时候，它就会逃离这个循环。
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        // lastVersion小于当前version，出等待队列
        while (this.version <= lastVersion) {
            if (remainingWaitMs != 0)
                // 阻塞等待sender线程更新元数据成功
                wait(remainingWaitMs);
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                // 超过最大等待的时间直接抛异常
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.containsAll(topics))
            requestUpdate();
        this.topics.clear();
        this.topics.addAll(topics);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<String>(this.topics);
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.contains(topic);
    }

    /**
     * Update the cluster metadata
     *
     * 在KafkaProducer初始化的时候，并没有真正的去某一个broker上去拉取元数据的，但是他肯定是对集群元数据做了一个初始化的，
     * 把你配置的那些broker地址转化为了Node，放在Cluster对象实例里
     * getClusterForCurrentTopics(cluster)
     */
    public synchronized void update(Cluster cluster, long now) {
        this.needUpdate = false;
        // 最后更新时间
        this.lastRefreshMs = now;
        // 最后成功刷新时间
        this.lastSuccessfulRefreshMs = now;
        // 版本+1
        this.version += 1;

        // 监听器通知
        for (Listener listener: listeners)
            listener.onMetadataUpdate(cluster);

        // Do this after notifying listeners as subscribed topics' list can be changed by listeners
        // 更新metadata 维护的cluster对象，换成了响应回来的那个
        this.cluster = this.needMetadataForAllTopics ? getClusterForCurrentTopics(cluster) : cluster;

        // 唤醒等待的主线程
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }
    
    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        if (cluster != null) {
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics);

            for (String topic : this.topics) {
                partitionInfos.addAll(cluster.partitionsForTopic(topic));
            }
            nodes = cluster.nodes();
        }
        return new Cluster(nodes, partitionInfos, unauthorizedTopics);
    }
}
