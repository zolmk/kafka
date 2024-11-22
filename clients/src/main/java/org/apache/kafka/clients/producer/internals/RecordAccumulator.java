/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public class RecordAccumulator {

    private final LogContext logContext;
    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;

    /** 用来记录有多少个消息正在添加流程中 */
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final Compression compression;
    private final int lingerMs;
    /**
     * 指数重试退避算法实现类
     */
    private final ExponentialBackoff retryBackoff;
    /**
     * 请求发送的超时时间 默认30s
     */
    private final int deliveryTimeoutMs;

    /**
     * 标记分区暂时不可用的延迟阈值
     */
    private final long partitionAvailabilityTimeoutMs;  // latency threshold for marking partition temporary unavailable
    private final boolean enableAdaptivePartitioning;
    private final BufferPool free;
    private final Time time;
    private final ApiVersions apiVersions;
    private final ConcurrentMap<String /*topic*/, TopicInfo> topicInfoMap = new CopyOnWriteMap<>();
    /**
     *
     */
    private final ConcurrentMap<Integer /*nodeId*/, NodeLatencyStats> nodeStats = new CopyOnWriteMap<>();
    private final IncompleteBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private final Map<String, Integer> nodesDrainIndex;
    private final TransactionManager transactionManager;
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE; // the earliest time (absolute) a batch will expire.

    /**
     * Create a new record accumulator
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param partitionerConfig Partitioner config
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             PartitionerConfig partitionerConfig,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.logContext = logContext;
        this.log = logContext.logger(RecordAccumulator.class);
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        // 每一个Batch的最大大小，默认16384=16KB
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoff = new ExponentialBackoff(retryBackoffMs,
                CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
                retryBackoffMaxMs,
                CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.enableAdaptivePartitioning = partitionerConfig.enableAdaptivePartitioning;
        this.partitionAvailabilityTimeoutMs = partitionerConfig.partitionAvailabilityTimeoutMs;
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        this.apiVersions = apiVersions;
        nodesDrainIndex = new HashMap<>();
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    /**
     * Create a new record accumulator with default partitioner config
     *
     * @param logContext The log context used for logging
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param retryBackoffMaxMs The upper bound of the retry backoff time.
     * @param deliveryTimeoutMs An upper bound on the time to report success or failure on record delivery
     * @param metrics The metrics
     * @param metricGrpName The metric group name
     * @param time The time instance to use
     * @param apiVersions Request API versions for current connected brokers
     * @param transactionManager The shared transaction state object which tracks producer IDs, epochs, and sequence
     *                           numbers per partition.
     * @param bufferPool The buffer pool
     */
    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             Compression compression,
                             int lingerMs,
                             long retryBackoffMs,
                             long retryBackoffMaxMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this(logContext,
            batchSize,
            compression,
            lingerMs,
            retryBackoffMs,
            retryBackoffMaxMs,
            deliveryTimeoutMs,
            new PartitionerConfig(),
            metrics,
            metricGrpName,
            time,
            apiVersions,
            transactionManager,
            bufferPool);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        metrics.addMetric(
            metrics.metricName("waiting-threads", metricGrpName,
                "The number of user threads blocked waiting for buffer memory to enqueue their records"),
            (config, now) -> free.queued());

        metrics.addMetric(
            metrics.metricName("buffer-total-bytes", metricGrpName,
                "The maximum amount of buffer memory the client can use (whether or not it is currently used)."),
            (config, now) -> free.totalMemory());

        metrics.addMetric(
            metrics.metricName("buffer-available-bytes", metricGrpName,
                "The total amount of buffer memory that is not being used (either unallocated or in the free list)."),
            (config, now) -> free.availableMemory());
    }

    private void setPartition(AppendCallbacks callbacks, int partition) {
        if (callbacks != null)
            callbacks.setPartition(partition);
    }

    /**
     * Check if partition concurrently changed, or we need to complete previously disabled partition change.
     *
     * @param topic The topic
     * @param topicInfo The topic info
     * @param partitionInfo The built-in partitioner's partition info
     * @param deque The partition queue
     * @param nowMs The current time, in milliseconds
     * @param cluster THe cluster metadata
     * @return 'true' if partition changed and we need to get new partition info and retry,
     *         'false' otherwise
     */
    private boolean partitionChanged(String topic,
                                     TopicInfo topicInfo,
                                     BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                     Deque<ProducerBatch> deque, long nowMs,
                                     Cluster cluster) {
        // 是否更新了粘性分区器
        if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
            log.trace("Partition {} for topic {} switched by a concurrent append, retrying",
                    partitionInfo.partition(), topic);
            return true;
        }

        // We might have disabled partition switch if the queue had incomplete batches.
        // Check if all batches are full now and switch .
        if (allBatchesFull(deque)) {
            topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, 0, cluster, true);
            if (topicInfo.builtInPartitioner.isPartitionChanged(partitionInfo)) {
                log.trace("Completed previously disabled switch for topic {} partition {}, retrying",
                        topic, partitionInfo.partition());
                return true;
            }
        }

        return false;
    }

    /**
     *
     * logic
     * sync()
     * {}
     * logic
     * sync()
     * 分段锁的设计虽然提高了并发性能，但是却会导致乱序问题
     * 一般影响不大
     *
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param topic The topic to which this record is being sent
     * @param partition The partition to which this record is being sent or RecordMetadata.UNKNOWN_PARTITION
     *                  if any partition could be used
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callbacks The callbacks to execute
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     * @param abortOnNewBatch A boolean that indicates returning before a new batch is created and
     *                        running the partitioner's onNewBatch method before trying to append again
     * @param nowMs The current time, in milliseconds
     * @param cluster The cluster metadata
     */
    public RecordAppendResult append(String topic,
                                     int partition,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     AppendCallbacks callbacks,
                                     long maxTimeToBlock,
                                     // abort 中止 流产
                                     boolean abortOnNewBatch,
                                     long nowMs,
                                     Cluster cluster) throws InterruptedException {
        // 1.根据topic name 获取到topicInfo对象
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(topic, k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize)));

        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        // 我们跟踪附加线程的数量，以确保我们不会错过abortIncompleteBatches() 中的批次。
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // encounter 遇到 碰到
            // Loop to retry in case we encounter partitioner's race conditions.
            while (true) {
                // If the message doesn't have any partition affinity, so we pick a partition based on the broker
                // availability and performance.  Note, that here we peek current partition before we hold the
                // deque lock, so we'll need to make sure that it's not changed while we were waiting for the
                // deque lock.
                // 如果消息没有任何分区关联，那么我们根据代理可用性和性能选择一个分区。
                // 注意，在这里我们在持有deque锁之前查看当前分区，所以我们需要确保在等待deque锁时它没有改变。
                final BuiltInPartitioner.StickyPartitionInfo partitionInfo;
                final int effectivePartition;
                if (partition == RecordMetadata.UNKNOWN_PARTITION) {
                    // 一般来说，当消息的key为null时，分区才可能为RecordMetadata.UNKNOWN_PARTITION
                    partitionInfo = topicInfo.builtInPartitioner.peekCurrentPartitionInfo(cluster);
                    effectivePartition = partitionInfo.partition();
                } else {
                    partitionInfo = null;
                    effectivePartition = partition;
                }

                // Now that we know the effective partition, let the caller know.
                setPartition(callbacks, effectivePartition);
                // 获取分区对应的Deque
                // check if we have an in-progress batch
                Deque<ProducerBatch> dq = topicInfo.batches.computeIfAbsent(effectivePartition, k -> new ArrayDeque<>());
                synchronized (dq) {
                    // 获取锁定后，验证分区是否未更改并重试。
                    // After taking the lock, validate that the partition hasn't changed and retry.
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                    // 首次返回为空
                    if (appendResult != null) {
                        // If queue has incomplete batches we disable switch (see comments in updatePartitionInfo).
                        // 如果入队成功，则先切换分区信息（换个分区生产，以负载均衡），然后返回记录添加结果
                        boolean enableSwitch = allBatchesFull(dq);
                        topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                        return appendResult;
                    }
                }

                // 没有在流程中的record batch，尝试稍后去创建一个新batch
                // we don't have an in-progress record batch try to allocate a new batch
                if (abortOnNewBatch) {
                    // Return a result that will cause another call to append.
                    return new RecordAppendResult(null, false, false, true, 0);
                }

                // 添加到Deque失败
                if (buffer == null) {
                    // buffer为空，走到这里是因为需要创建新的RecordBatch，所以需要分配缓存
                    byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
                    int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression.type(), key, value, headers));
                    log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, topic, effectivePartition, maxTimeToBlock);
                    // This call may block if we exhausted buffer space.
                    buffer = free.allocate(size, maxTimeToBlock);
                    // Update the current time in case the buffer allocation blocked above.
                    // NOTE: getting time may be expensive, so calling it under a lock
                    // should be avoided.
                    // 如果上面的缓冲区分配被阻塞，请更新当前时间。注意: 获取时间可能很昂贵，因此应避免在锁定下调用它。
                    nowMs = time.milliseconds();
                }

                synchronized (dq) {
                    // After taking the lock, validate that the partition hasn't changed and retry.
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    RecordAppendResult appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks, buffer, nowMs);
                    // Set buffer to null, so that deallocate doesn't return it back to free pool, since it's used in the batch.
                    if (appendResult.newBatchCreated)
                        buffer = null;
                    // If queue has incomplete batches we disable switch (see comments in updatePartitionInfo).
                    boolean enableSwitch = allBatchesFull(dq);
                    topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                    return appendResult;
                }
            }
        } finally {
            // 这里为什么要关闭buffer？为了防止内存泄漏，如果上面的代码抛出了异常，buffer没有被回收，就可能导致内存泄露。
            // 同时，只有当创建batch成功时，buffer会被置为null，其他情况下，可能buffer被重复创建了，需要放入池中
            free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * Append a new batch to the queue
     *
     * @param topic The topic
     * @param partition The partition (cannot be RecordMetadata.UNKNOWN_PARTITION)
     * @param dq The queue
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param headers the Headers for the record
     * @param callbacks The callbacks to execute
     * @param buffer The buffer for the new batch
     * @param nowMs The current time, in milliseconds
     */
    private RecordAppendResult appendNewBatch(String topic,
                                              int partition,
                                              Deque<ProducerBatch> dq,
                                              long timestamp,
                                              byte[] key,
                                              byte[] value,
                                              Header[] headers,
                                              AppendCallbacks callbacks,
                                              ByteBuffer buffer,
                                              long nowMs) {
        assert partition != RecordMetadata.UNKNOWN_PARTITION;
        // 尝试添加到Deque队尾
        RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
        if (appendResult != null) {
            // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
            return appendResult;
        }

        // 新建一个ProducerBatch
        MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, apiVersions.maxUsableProduceMagic());
        ProducerBatch batch = new ProducerBatch(new TopicPartition(topic, partition), recordsBuilder, nowMs);
        FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                callbacks, nowMs));

        dq.addLast(batch);
        incomplete.add(batch);

        return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false, batch.estimatedSizeInBytes());
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer, byte maxUsableMagic) {
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedVersionException("Attempting to use idempotence with a broker which does not " +
                "support the required message format (v2). The broker must be version 0.11 or later.");
        }
        return MemoryRecords.builder(buffer, maxUsableMagic, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     * Check if all batches in the queue are full.
     */
    private boolean allBatchesFull(Deque<ProducerBatch> deque) {
        // Only the last batch may be incomplete, so we just check that.
        ProducerBatch last = deque.peekLast();
        return last == null || last.isFull();
    }

     /**
      * 尝试追加到ProducerBatch。如果它是满的，我们返回null，并创建一个新的批次。我们还关闭记录追加批处理，
      * 以释放压缩缓冲区等资源。在以下情况之一 (以先到者为准)，批次将完全关闭 (即记录批次标头将被写入并构建内存记录):
      * 就在发送之前，如果过期，或者当生产者关闭时。
      *
     *  Try to append to a ProducerBatch.
     *
     *  If it is full, we return null and a new batch is created. We also close the batch for record appends to free up
     *  resources like compression buffers. The batch will be fully closed (ie. the record batch headers will be written
     *  and memory records built) in one of the following cases (whichever comes first): right before send,
     *  if it is expired, or when the producer is closed.
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        if (closed)
            throw new KafkaException("Producer closed while send in progress");
        // 首次添加，队列为空，这里last为空
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            // estimate 估计
            int initialBytes = last.estimatedSizeInBytes();
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null) {
                last.closeForRecordAppends();
            } else {
                int appendedBytes = last.estimatedSizeInBytes() - initialBytes;
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false, appendedBytes);
            }
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);
    }

    public void resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE;
    }

    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        if (batch.createdMs + deliveryTimeoutMs  > 0) {
            // the non-negative check is to guard us against potential overflow due to setting
            // a large value for deliveryTimeoutMs
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * Get a list of batches which have been sitting in the accumulator too long and need to be expired.
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                // expire the batches in the order of sending
                synchronized (deque) {
                    while (!deque.isEmpty()) {
                        ProducerBatch batch = deque.getFirst();
                        if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
                            // batch到投递超时
                            deque.poll();
                            batch.abortRecordAppends();
                            expiredBatches.add(batch);
                        } else {
                            // 未过期，更新 nextBatchExpiryTimeMs 全局变量
                            maybeUpdateNextBatchExpiryTime(batch);
                            break;
                        }
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * Re-enqueue the given record batch in the accumulator. In Sender.completeBatch method, we check
     * whether the batch has reached deliveryTimeoutMs or not. Hence we do not do the delivery timeout check here.
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            if (transactionManager != null)
                insertInSequenceOrder(deque, batch);
            else
                deque.addFirst(batch);
        }
    }

    /**
     * Split the big batch that has been rejected and reenqueue the split batches in to the accumulator.
     * @return the number of split batches.
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // Reset the estimated compression ratio to the initial value or the big batch compression ratio, whichever
        // is bigger. There are several different ways to do the reset. We chose the most conservative one to ensure
        // the split doesn't happen too often.
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression.type(),
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    // We will have to do extra work to ensure the queue is in order when requests are being retried and there are
    // multiple requests in flight to that partition. If the first in flight request fails to append, then all the
    // subsequent in flight requests will also fail because the sequence numbers will not be accepted.
    //
    // Further, once batches are being retried, we are reduced to a single in flight request for that partition. So when
    // the subsequent batches come back in sequence order, they will have to be placed further back in the queue.
    //
    // Note that this assumes that all the batches in the queue which have an assigned sequence also have the current
    // producer id. We will not attempt to reorder messages if the producer id has changed, we will throw an
    // IllegalStateException instead.
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // When we are re-enqueueing and have enabled idempotence, the re-enqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        if (!transactionManager.hasInflightBatches(batch.topicPartition))
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // The incoming batch can't be inserted at the front of the queue without violating the sequence ordering.
            // This means that the incoming batch should be placed somewhere further back.
            // We need to find the right place for the incoming batch and insert it there.
            // We will only enter this branch if we have multiple inflights sent to different brokers and we need to retry
            // the inflight batches.
            //
            // Since we reenqueue exactly one batch a time and ensure that the queue is ordered by sequence always, it
            // is a simple linear scan of a subset of the in flight batches to find the right place in the queue each time.
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // Either we have reached a point where there are batches without a sequence (ie. never been drained
            // and are hence in order by default), or the batch at the front of the queue has a sequence greater
            // than the incoming batch. This is the right place to add the incoming batch.
            deque.addFirst(batch);

            // Now we have to re insert the previously queued batches in the right order.
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // At this point, the incoming batch has been queued in the correct place according to its sequence.
        } else {
            deque.addFirst(batch);
        }
    }

    /**
     * Add the leader to the ready nodes if the batch is ready
     *
     * @param exhausted 'true' is the buffer pool is exhausted
     * @param part The partition
     * @param leader The leader for the partition
     * @param waitedTimeMs How long batch waited
     * @param backingOff Is backing off
     * @param backoffAttempts Number of attempts for calculating backoff delay
     * @param full Is batch full
     * @param nextReadyCheckDelayMs The delay for next check
     * @param readyNodes The set of ready nodes (to be filled in)
     * @return The delay for next check
     */
    private long batchReady(boolean exhausted, TopicPartition part, Node leader,
                            long waitedTimeMs, boolean backingOff, int backoffAttempts,
                            boolean full, long nextReadyCheckDelayMs, Set<Node> readyNodes) {
        if (!readyNodes.contains(leader) && !isMuted(part)) {
            // 计算等待时间，如果 backingOff 为true，则使用 指数退避算法计算出的退避时间，否则，使用 用户配置的 批次最大停留时间 lingerMs
            long timeToWaitMs = backingOff ? retryBackoff.backoff(backoffAttempts > 0 ? backoffAttempts - 1 : 0) : lingerMs;
            // 判断当前批次是否过期，如果已等待时间大于所计算的等待时间，则认为批次已过期。意思就是说不能再等了，这次必须发送出去
            boolean expired = waitedTimeMs >= timeToWaitMs;
            // 事务是否完成
            boolean transactionCompleting = transactionManager != null && transactionManager.isCompleting();
            // 是否可以发送
            /**
             * 满足什么条件后，批次可以发送出去？
             * 1. 队列大小大于1或队头ProducerBatch满了的时候
             * 2. 即将过期的批次
             * 3. 缓存池耗尽的时候
             * 4. 关闭Sender线程的时候
             */
            boolean sendable = full
                    || expired
                    || exhausted
                    || closed
                    || flushInProgress()
                    || transactionCompleting;

            // 如果可以发送 并且 不回退，则将当前节点加入到就绪节点中
            if (sendable && !backingOff) {
                readyNodes.add(leader);
            } else {
                // 当前批次发送条件不满足，则计算 还要等待的时间
                long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                // Note that this results in a conservative estimate since an un-sendable partition may have
                // a leader that will later be found to have sendable data. However, this is good enough
                // since we'll just wake up and then sleep again for the remaining time.
                nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
            }
        }
        return nextReadyCheckDelayMs;
    }

    /**
     * 遍历分区以查看哪些已准备好批处理，并将这些分区的领导者收集到就绪节点集合中。
     * 如果分区没有引出线，则将该主题添加到没有引出线的主题集合中。此函数还计算自适应分区的统计信息。
     * Iterate over partitions to see which one have batches ready and collect leaders of those
     * partitions into the set of ready nodes.  If partition has no leader, add the topic to the set
     * of topics with no leader.  This function also calculates stats for adaptive partitioning.
     *
     * @param metadataSnapshot      The cluster metadata
     * @param nowMs                 The current time
     * @param topic                 The topic
     * @param topicInfo             The topic info
     * @param nextReadyCheckDelayMs The delay for next check
     * @param readyNodes            The set of ready nodes (to be filled in)
     * @param unknownLeaderTopics   The set of topics with no leader (to be filled in)
     * @return The delay for next check
     */
    private long partitionReady(MetadataSnapshot metadataSnapshot, long nowMs, String topic,
                                TopicInfo topicInfo,
                                long nextReadyCheckDelayMs, Set<Node> readyNodes, Set<String> unknownLeaderTopics) {
        ConcurrentMap<Integer, Deque<ProducerBatch>> batches = topicInfo.batches;
        // Collect the queue sizes for available partitions to be used in adaptive partitioning.
        // 记录分区对应的队列大小
        int[] queueSizes = null;
        // 记录存在leader节点的分区ID
        int[] partitionIds = null;
        if (enableAdaptivePartitioning && batches.size() >= metadataSnapshot.cluster().partitionsForTopic(topic).size()) {
            // We don't do adaptive partitioning until we scheduled at least a batch for all
            // partitions (i.e. we have the corresponding entries in the batches map), we just
            // do uniform.  The reason is that we build queue sizes from the batches map,
            // and if an entry is missing in the batches map, then adaptive partitioning logic
            // won't know about it and won't switch to it.
            queueSizes = new int[batches.size()];
            partitionIds = new int[queueSizes.length];
        }

        int queueSizesIndex = -1;
        // 如果为true说明内存池内存不足
        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<Integer, Deque<ProducerBatch>> entry : batches.entrySet()) {
            TopicPartition part = new TopicPartition(topic, entry.getKey());
            // Advance queueSizesIndex so that we properly index available
            // partitions.  Do it here so that it's done for all code paths.
            // 获取当前分区的leader节点
            Node leader = metadataSnapshot.cluster().leaderFor(part);
            if (leader != null && queueSizes != null) {
                ++queueSizesIndex;
                assert queueSizesIndex < queueSizes.length;
                partitionIds[queueSizesIndex] = part.partition();
            }

            Deque<ProducerBatch> deque = entry.getValue();

            // 已等待的时间（毫秒）
            final long waitedTimeMs;
            // backing off 后退 返回
            final boolean backingOff;

            final int backoffAttempts;
            final int dequeSize;
            final boolean full;

            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(part);

            // This loop is especially hot with large partition counts. So -

            // 1. We should avoid code that increases synchronization between application thread calling
            // send(), and background thread running runOnce(), see https://issues.apache.org/jira/browse/KAFKA-16226

            // 2. We are careful to only perform the minimum required inside the
            // synchronized block, as this lock is also used to synchronize producer threads
            // attempting to append() to a partition/batch.

            synchronized (deque) {
                // Deques are often empty in this path, esp with large partition counts,
                // so we exit early if we can.
                ProducerBatch batch = deque.peekFirst();
                if (batch == null) {
                    continue;
                }
                // 已等待的时间（毫秒）
                waitedTimeMs = batch.waitedTimeMs(nowMs);
                batch.maybeUpdateLeaderEpoch(leaderEpoch);
                // 是否退避（再等一会发），关于这里退避的理解：
                // 之前有学过在计网中学过指数退避算法，是因为网络拥塞，发送端不得不慢点发
                // 这里也可以理解为客户端与服务器连接不通，需要慢点发送，但是这个发送间隔不是固定的，
                // 是动态的，这里利用了指数退避算法来动态调整重试间隔。
                backingOff = shouldBackoff(batch.hasLeaderChangedForTheOngoingRetry(), batch, waitedTimeMs);
                backoffAttempts = batch.attempts();
                dequeSize = deque.size();
                // 队列是否有可发送（已满）的批次
                full = dequeSize > 1 || batch.isFull();
            }

            if (leader == null) {
                // This is a partition for which leader is not known, but messages are available to send.
                // Note that entries are currently not removed from batches when deque is empty.
                // 统计存在分区Leader为空的所有topic
                unknownLeaderTopics.add(part.topic());
            } else {
                if (queueSizes != null)
                    queueSizes[queueSizesIndex] = dequeSize;
                if (partitionAvailabilityTimeoutMs > 0) {
                    // Check if we want to exclude the partition from the list of available partitions
                    // if the broker hasn't responded for some time.
                    // 更新节点延迟统计信息
                    NodeLatencyStats nodeLatencyStats = nodeStats.get(leader.id());
                    if (nodeLatencyStats != null) {
                        /**
                         * 注意: 读取指标之间没有同步，因此我们首先读取就绪时间，以避免在更新指标时读取分区时意外标记为不可用。
                         */
                        // NOTE: there is no synchronization between reading metrics,
                        // so we read ready time first to avoid accidentally marking partition
                        // unavailable if we read while the metrics are being updated.
                        long readyTimeMs = nodeLatencyStats.readyTimeMs;
                        // 如果准备时间减去排出时间大于分区可用性超时时间，则认为分区不可用
                        if (readyTimeMs - nodeLatencyStats.drainTimeMs > partitionAvailabilityTimeoutMs)
                            --queueSizesIndex;
                    }
                }

                /**
                 * 这里用到了停留时间 lingerMs
                 */
                nextReadyCheckDelayMs = batchReady(exhausted, part, leader, waitedTimeMs, backingOff,
                    backoffAttempts, full, nextReadyCheckDelayMs, readyNodes);
            }
        }

        // We've collected the queue sizes for partitions of this topic, now we can calculate
        // load stats.  NOTE: the stats are calculated in place, modifying the
        // queueSizes array.
        /**
         * 我们已经收集了本主题分区的队列大小，现在我们可以计算负载统计信息了。注意: 统计信息计算到位，修改队列大小数组。
         */
        topicInfo.builtInPartitioner.updatePartitionLoadStats(queueSizes, partitionIds, queueSizesIndex + 1);
        return nextReadyCheckDelayMs;
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(MetadataSnapshot metadataSnapshot, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();
        // Go topic by topic so that we can get queue sizes for partitions in a topic and calculate
        // cumulative frequency table (used in partitioner).
        for (Map.Entry<String, TopicInfo> topicInfoEntry : this.topicInfoMap.entrySet()) {
            final String topic = topicInfoEntry.getKey();
            nextReadyCheckDelayMs = partitionReady(metadataSnapshot, nowMs, topic, topicInfoEntry.getValue(), nextReadyCheckDelayMs, readyNodes, unknownLeaderTopics);
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * Check whether there are any batches which haven't been drained
     */
    public boolean hasUndrained() {
        for (TopicInfo topicInfo : topicInfoMap.values()) {
            for (Deque<ProducerBatch> deque : topicInfo.batches.values()) {
                synchronized (deque) {
                    if (!deque.isEmpty())
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * 计算是否应该退避，即当前批次 还可以再等会
     * @param hasLeaderChanged
     * @param batch
     * @param waitedTimeMs
     * @return
     */
    private boolean shouldBackoff(boolean hasLeaderChanged, final ProducerBatch batch, final long waitedTimeMs) {
        // 已尝试次数大于0 且 已等待时间 小于 计算得出的重试间隔，那么就再等会
        boolean shouldWaitMore =
          // 如果是第一次发送，则attempts返回的是0，所以shouldWaitMore为false
          batch.attempts() > 0
            && waitedTimeMs < retryBackoff.backoff(batch.attempts() - 1);
        // 如果partition的leader未变更 且 应该等更长时间，则应该 回退（backoff）
        boolean shouldBackoff = !hasLeaderChanged && shouldWaitMore;
        if (log.isTraceEnabled()) {
            if (shouldBackoff) {
                log.trace(
                    "For {}, will backoff", batch);
            } else {
                log.trace(
                    "For {}, will not backoff, shouldWaitMore {}, hasLeaderChanged {}", batch,
                    shouldWaitMore, hasLeaderChanged);
            }
        } else if (log.isDebugEnabled() && hasLeaderChanged) {
            // Add less-verbose log at DEBUG.
            log.debug("For {}, leader has changed, hence skipping backoff.", batch);
        }
        return shouldBackoff;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch;
        if (transactionManager != null) {
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // we cannot send the batch until we have refreshed the producer id
                return true;

            if (!first.hasSequence()) {
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    // Don't drain any new batches while the partition has in-flight batches with a different epoch
                    // and/or producer ID. Otherwise, a batch with a new epoch and sequence number
                    // 0 could be written before earlier batches complete, which would cause out of sequence errors
                    return true;
                }

                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    // Don't drain any new batches while the state of previous sequence numbers
                    // is unknown. The previous batches would be unknown if they were aborted
                    // on the client after being sent to the broker at least once.
                    return true;
            }

            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            // If the queued batch already has an assigned sequence, then it is being retried.
            // In this case, we wait until the next immediate batch is ready and drain that.
            // We only move on when the next in line batch is complete (either successfully or due to
            // a fatal broker error). This effectively reduces our in flight request count to 1.
            return firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                    && first.baseSequence() != firstInFlightSequence;
        }
        return false;
    }

    /**
     * 排出对应Node的所有ProducerBatches
     * @param metadataSnapshot
     * @param node
     * @param maxSize
     * @param now
     * @return
     */
    private List<ProducerBatch> drainBatchesForOneNode(MetadataSnapshot metadataSnapshot,
                                                       Node node, int maxSize, long now) {
        // 记录当前Node可排出的数据大小
        int size = 0;
        /**
         * 获取当前Node上有哪些Partition
         */
        List<PartitionInfo> parts = metadataSnapshot.cluster().partitionsForNode(node.id());
        List<ProducerBatch> ready = new ArrayList<>();
        if (parts.isEmpty())
            return ready;
        /* to make starvation less likely each node has it's own drainIndex */
        // 为了减少饥饿的可能性，每个节点都有自己的drainIndex
        int drainIndex = getDrainIndex(node.idString());
        int start = drainIndex = drainIndex % parts.size();
        /**
         * 这里的 do {} while()循环代替了for循环遍历parts去获取partition对应的ProducerBatch
         * 这样做的目的是为了减少饥饿的可能性？
         * TODO：注意
         * 这里的返回结果ready列表的大小是有限的，取决于最大的请求大小，所以如果每次都从parts的0号索引开始，则会导致位于后方的分区得不到执行
         * 所以这里维护了一个drainIndex来解决该问题，减少饥饿问题。
         */
        do {
            PartitionInfo part = parts.get(drainIndex);
            // topic partition 用topic和partition来做标识
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            // 更新节点的drainIndex
            updateDrainIndex(node.idString(), drainIndex);
            drainIndex = (drainIndex + 1) % parts.size();
            // Only proceed if the partition has no in-flight batches.
            // 是否分区被禁音 如果分区被禁音了，则继续处理下一个
            // TODO 什么情况下分区会被禁音？
            if (isMuted(tp))
                continue;
            // 获取TopicPartition对应的Deque
            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                continue;

            // 从元数据信息中获取当前partition的 Leader 任期（epoch）
            OptionalInt leaderEpoch = metadataSnapshot.leaderEpochFor(tp);

            final ProducerBatch batch;
            synchronized (deque) {
                // invariant: !isMuted(tp,now) && deque != null
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    continue;

                // first != null
                // Only drain the batch if it is not during backoff period.
                first.maybeUpdateLeaderEpoch(leaderEpoch);
                // 是否应该回退，如果leader变更，则一定不回退
                // 如果是第一次发送该批次，则shouldBackoff返回false，不会进行退避
                if (shouldBackoff(first.hasLeaderChangedForTheOngoingRetry(), first, first.waitedTimeMs(now)))
                    continue;

                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // there is a rare case that a single batch size is larger than the request size due to
                    // compression; in this case we will still eventually send this batch in a single request
                    // 数据量超过了最大请求大小，则直接退出循环
                    break;
                } else {
                    // 判断是否应该停止该分区流出数据，主要是事务相关的判断
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        break;
                }

                batch = deque.pollFirst();

                boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                ProducerIdAndEpoch producerIdAndEpoch =
                    transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                if (producerIdAndEpoch != null && !batch.hasSequence()) {
                    // If the producer id/epoch of the partition do not match the latest one
                    // of the producer, we update it and reset the sequence. This should be
                    // only done when all its in-flight batches have completed. This is guarantee
                    // in `shouldStopDrainBatchesForPartition`.
                    /**
                     * 如果分区的生产者idepoch与生产者的最新一个不匹配，我们更新它并重置序列。
                     * 只有在所有进行中的批次都已完成时，才应执行此操作。这是 'shouldStopDrainBatchesForPartition' 中的保证。
                     */
                    transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                    // If the batch already has an assigned sequence, then we should not change the producer id and
                    // sequence number, since this may introduce duplicates. In particular, the previous attempt
                    // may actually have been accepted, and if we change the producer id and sequence here, this
                    // attempt will also be accepted, causing a duplicate.
                    //
                    // Additionally, we update the next sequence number bound for the partition, and also have
                    // the transaction manager track the batch so as to ensure that sequence ordering is maintained
                    // even if we receive out of order responses.
                    /**
                     * 如果批次已经具有分配的序列，那么我们不应该更改生产者id和序列号，因为这可能会引入重复。
                     * 特别是，先前的尝试实际上可能已被接受，并且如果我们在此处更改生产者id和序列，则此尝试也将被接受，从而导致重复。
                     * 此外，我们更新分区的下一个序列号，并且还让事务管理器跟踪批处理，以确保即使我们收到无序响应，也可以保持序列排序。
                     */
                    batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                    transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                    log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " +
                            "{} being sent to partition {}", producerIdAndEpoch.producerId,
                        producerIdAndEpoch.epoch, batch.baseSequence(), tp);

                    transactionManager.addInFlightBatch(batch);
                }
            }

            // the rest of the work by processing outside the lock
            // close() is particularly expensive
            batch.close();
            size += batch.records().sizeInBytes();
            ready.add(batch);
            // 设置batch排出时间
            batch.drained(now);
        } while (start != drainIndex);
        return ready;
    }

    private int getDrainIndex(String idString) {
        return nodesDrainIndex.computeIfAbsent(idString, s -> 0);
    }

    private void updateDrainIndex(String idString, int drainIndex) {
        nodesDrainIndex.put(idString, drainIndex);
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit
     * within the specified size on a per-node basis. This method attempts to avoid choosing the same
     * topic-node over and over.
     *
     * @param metadataSnapshot  The current cluster metadata
     * @param nodes             The list of node to drain
     * @param maxSize           The maximum number of bytes to drain
     * @param now               The current unix time in milliseconds
     * @return A list of {@link ProducerBatch} for each node specified with total size less than the
     * requested maxSize.
     */
    public Map<Integer, List<ProducerBatch>> drain(MetadataSnapshot metadataSnapshot, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            // 获取节点所有分区的批记录
            List<ProducerBatch> ready = drainBatchesForOneNode(metadataSnapshot, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    // can drain 排出
    public void updateNodeLatencyStats(Integer nodeId, long nowMs, boolean canDrain) {
        // Don't bother with updating stats if the feature is turned off.
        if (partitionAvailabilityTimeoutMs <= 0)
            return;

        /**
         * 当发送方得到一个节点 (由ready() 函数返回)，该节点有数据要发送，但该节点还没有准备好 (因此我们不能排出数据)，
         * 我们只更新准备时间，然后，差异将反映节点未准备好发送数据的时间。然后，我们可以暂时从可用分区列表中删除节点处理的分区，
         * 以便分区器不会选择此分区。注意: 指标更新没有同步，因此首先更新drainTimeMs，以避免在读取器在更新之间获取值时意外
         * 将分区标记为不可用。
         */
        // When the sender gets a node (returned by the ready() function) that has data to send
        // but the node is not ready (and so we cannot drain the data), we only update the
        // ready time, then the difference would reflect for how long a node wasn't ready
        // to send the data.  Then we can temporarily remove partitions that are handled by the
        // node from the list of available partitions so that the partitioner wouldn't pick
        // this partition.
        // NOTE: there is no synchronization for metric updates, so drainTimeMs is updated
        // first to avoid accidentally marking a partition unavailable if the reader gets
        // values between updates.
        NodeLatencyStats nodeLatencyStats = nodeStats.computeIfAbsent(nodeId, id -> new NodeLatencyStats(nowMs));
        if (canDrain)
            nodeLatencyStats.drainTimeMs = nowMs;
        nodeLatencyStats.readyTimeMs = nowMs;
    }

    /* Visible for testing */
    public NodeLatencyStats getNodeLatencyStats(Integer nodeId) {
        return nodeStats.get(nodeId);
    }

    /* Visible for testing */
    public BuiltInPartitioner getBuiltInPartitioner(String topic) {
        return topicInfoMap.get(topic).builtInPartitioner;
    }

    /**
     * The earliest absolute time a batch will expire (in milliseconds)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

      /* Visible for testing */
    public Deque<ProducerBatch> getDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.get(tp.topic());
        if (topicInfo == null)
            return null;
        return topicInfo.batches.get(tp.partition());
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(tp.topic(),
                k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize)));
        return topicInfo.batches.computeIfAbsent(tp.partition(), k -> new ArrayDeque<>());
    }

    BuiltInPartitioner createBuiltInPartitioner(LogContext logContext, String topic, int stickyBatchSize) {
        return new BuiltInPartitioner(logContext, topic, stickyBatchSize);
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // Only deallocate the batch if it is not a split batch because split batch are allocated outside the
        // buffer pool.
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * Package private for unit test. Get the buffer pool remaining size in bytes.
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }

    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // Obtain a copy of all of the incomplete ProduceRequestResult(s) at the time of the flush.
            // We must be careful not to hold a reference to the ProduceBatch(s) so that garbage
            // collection can occur on the contents.
            // The sender will remove ProducerBatch(s) from the original incomplete collection.
            for (ProduceRequestResult result : this.incomplete.requestResults())
                result.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * Check whether there are any pending batches (whether sent or unsent).
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.topicInfoMap.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

    /**
     * Abort all incomplete batches (whether they have been sent or not)
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            synchronized (dq) {
                batch.abortRecordAppends();
                dq.remove(batch);
            }
            batch.abort(reason);
            deallocate(batch);
        }
    }

    /**
     * Abort any batches which have not been drained
     */
    void abortUndrainedBatches(RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            boolean aborted = false;
            synchronized (dq) {
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    batch.abortRecordAppends();
                    dq.remove(batch);
                }
            }
            if (aborted) {
                batch.abort(reason);
                deallocate(batch);
            }
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
        this.free.close();
    }

    /**
     * Partitioner config for built-in partitioner
     */
    public static final class PartitionerConfig {
        private final boolean enableAdaptivePartitioning;
        private final long partitionAvailabilityTimeoutMs;

        /**
         * Partitioner config
         *
         * @param enableAdaptivePartitioning If it's true, partition switching adapts to broker load, otherwise partition
         *        switching is random.
         * @param partitionAvailabilityTimeoutMs If a broker cannot process produce requests from a partition
         *        for the specified time, the partition is treated by the partitioner as not available.
         *        If the timeout is 0, this logic is disabled.
         */
        public PartitionerConfig(boolean enableAdaptivePartitioning, long partitionAvailabilityTimeoutMs) {
            this.enableAdaptivePartitioning = enableAdaptivePartitioning;
            this.partitionAvailabilityTimeoutMs = partitionAvailabilityTimeoutMs;
        }

        public PartitionerConfig() {
            this(false, 0);
        }
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public static final class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;
        public final boolean abortForNewBatch;
        public final int appendedBytes;

        public RecordAppendResult(FutureRecordMetadata future,
                                  boolean batchIsFull,
                                  boolean newBatchCreated,
                                  boolean abortForNewBatch,
                                  int appendedBytes) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
            this.appendedBytes = appendedBytes;
        }
    }

    /*
     * The callbacks passed into append
     */
    public interface AppendCallbacks extends Callback {
        /**
         * Called to set partition (when append is called, partition may not be calculated yet).
         * @param partition The partition
         */
        void setPartition(int partition);
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public static final class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }

    /**
     * Per topic info.
     */
    private static class TopicInfo {
        public final ConcurrentMap<Integer /*partition*/, Deque<ProducerBatch>> batches = new CopyOnWriteMap<>();
        public final BuiltInPartitioner builtInPartitioner;

        public TopicInfo(BuiltInPartitioner builtInPartitioner) {
            this.builtInPartitioner = builtInPartitioner;
        }
    }

    /**
     * 用于自适应分区分配的每个节点的节点延迟统计信息可见用于测试
     * 延迟统计可以理解为：统计分区从准备好到数据排出的时间间隔，如果这个时间间隔过大，则可以认为分区不可用
     * Node latency stats for each node that are used for adaptive partition distribution
     * Visible for testing
     */
    public static final class NodeLatencyStats {
        /**
         * 节点上一次准备发送的时间
         */
        public volatile long readyTimeMs;  // last time the node had batches ready to send
        /**
         * 节点上一次排出数据的时间
         */
        public volatile long drainTimeMs;  // last time the node was able to drain batches

        NodeLatencyStats(long nowMs) {
            readyTimeMs = nowMs;
            drainTimeMs = nowMs;
        }
    }
}
