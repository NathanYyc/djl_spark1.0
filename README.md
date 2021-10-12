
# Deep Java Library (DJL) Spark

DJL-Spark利用了Deep Java Library作为单节点训练平台，利用Netty网络框架单独编写了一套在节点间进行数据传输的通信架构，实现了在Spark集群中进行分布式深度学习的任务。架构上采用了梯度分片式的参数服务器，该参数服务器负责对梯度分片进行处理、利用集群中的梯度分片计算总梯度并传回总梯度。

在具体实现上，DJL-Spark继承改写了DJL的ParameterServer类，使其支持了向参数服务器传输梯度的功能，并利用Dispatcher类进行梯度分片以及与参数服务器的交互。在参数服务器端则使用了Grads类进行梯度的同步工作。

## [Getting Started](docs/quick_start.md)

参考Run、Bert类中编写的代码，编写好模型后利用DistributedTrainer类进行分布式训练。使用时将DJL-Spark项目完整打包后上传至Spark平台中进行运行。

    注意：DJL-Spark的partitionNum必须与Spark集群中节点数量完全一致
