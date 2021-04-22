# 配置管理

​		支持灵活的配置是fire框架一大亮点，fire框架针对不同的引擎，提供了不同的配置文件，并且支持通过调用外部接口实现配置的动态覆盖。

### 1. 系统配置

fire框架内置了多个配置文件，用于应对多种引擎场景，分别是：

1）**fire.properties**：该配置文件中fire框架的总配置文件，位于fire-core包中，其中的配置主要是针对fire框架的，不含有spark或flink引擎的配置。

2）**spark.properties**：该配置文件是spark引擎的总配置文件，位于fire-spark包中，作为spark引擎任务的总配置文件。

3）**spark-core.properties**：该配置文件位于fire-spark包中，该配置文件用于配置spark core任务。

4）**spark-streaming.properties**：该配置文件位于fire-spark包中，主要用于spark streaming任务。

5）**structured-streaming.properties**：该配置文件位于fire-spark包中，用于进行structured streaming任务的配置。

6）**flink.properties**：该配置文件位于fire-flink包中，作为flink引擎的总配置文件。

7）**flink-streaming.properties**：该配置文件位于fire-flink包中，用于配置flink streaming任务。

8）**flink-batch.properties**：该配置文件位于fire-flink包中，用于配置flink批处理任务。

以上配置文件是fire框架内置的，用户无需关心。

### 2. 用户配置

#### 1）公共配置：

​		fire框架支持用户的公共配置，默认名称是**common.properties**。在公共配置文件中配置的内容将对所有任务生效。因此，可以考虑将一些公共配置项放到该文件中。

#### 2）任务配置：

​		用户配置需存放到代码的**src/main/resources**目录下，支持子目录存放。配置文件与任务通过名称进行自动绑定，一个任务一个配置文件。如果任务类名为：**Test**.scala，则对应的配置文件名称是：**Test**.properties。用户配置暂不支持一个任务多个配置文件。

### 3. 配置中心

​		为了提供灵活性，避免因配置修改而重新打包，fire框架提供了从接口获取配置信息，并覆盖用户同名配置的功能。该功能通常需要实时平台提供配置接口，用于重启覆盖参数。

### 4. 动态配置

​		动态配置是指可以在运行时动态获取及生效的配置，fire框架提供了相应的接口，平台通过调用该接口即可实现配置的热替换已经分布式分发。该功能目前仅支持spark引擎。

### 5. 优先级

​		fire.properties **<** spark.properties|flink.properties **<** spark-core.properties|spark-streaming.properties|structured-streaming.properties|flink-streaming.properties|flink-batch.properties  **<** common.properties **<** 用户配置文件 **<** 配置中心

### 6. 任务调优

​		fire支持所有的spark与flink参数，只需将相应的spark或flink引擎的参数配置到任务的配置文件中即可生效。甚至可以实现任务级别的覆盖flink-conf.yaml中的配置。

### 7. 分布式传递与获取

​		配置文件中的所有配置，会被fire框架分布式的分发给所有的spark executor和flink的task manager。用户无论是进行spark还是flink任务开发，都可以通过以下代码获取到配置信息：

```scala
// 获取布尔类型配置
this.conf.getBoolean("xxx", default = false)
// 获取Int类型配置
this.conf.getInt("xxx", default = -1)
```

​		通过this.conf.getXxx方式，可避免在flink的map等算子中通过open方法获取，同时在JobManager端、TaskManager端都能获取到。fire框架保证了配置的传递与一致性。

