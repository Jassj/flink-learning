/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * Skeleton code for the data stream walkthrough
 */
public class FraudDetectionJob {

	public static void main(String[] args) throws Exception {

		// 设置执行环境: 用于定义任务的属性, 创建数据源, 以及启动任务
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 创建数据源, Transaction: 无限循环生成信用卡模拟交易数据的数据源
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSource())
			.name("transactions");

		// 对事件进行分区 & 欺诈检测
		DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId) // 根据用户ID对流进行分区, 保证同一账户的所有交易记录被同一并发task执行
			.process(new FraudDetector()) // 对流绑定一个操作, 会对流上的每一个消息调用定义号的函数
			.name("fraud-detector");

		// 输出结果: 将DataStream写出到外部系统
		alerts
			.addSink(new AlertSink())
			.name("send-alerts");

		// 运行作业
		env.execute("Fraud Detection");
	}
}
