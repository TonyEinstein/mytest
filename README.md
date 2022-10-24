# mytest

java对hadoop生态的HDFS的操作案例实现。

<p align="center">
   <img alt="java" src="https://img.shields.io/badge/java-green.svg?style=plastic">
  <img alt="jdk 1.8" src="https://img.shields.io/badge/jdk-1.8-green.svg?style=plastic">
  <img alt="hadoop 2.7" src="https://img.shields.io/badge/hadoop-2.7-green.svg?style=plastic">
  <img alt="hbase" src="https://img.shields.io/badge/hbase-green.svg?style=plastic">
</p>


### 详细内容
- java对HDFS的mapreduce基本操作；
- 在HDFS上进行单词计数；


### 概念解析

1. mapper阶段:
根据使用者的意愿，mapper对输入的数据进行操作，选取需要的字段(这个字段可能来自原数据中的值，也可能是原数据中的字段，或者是一个新的字段)和值(这个值可能来自原数据，但是更可能来自经新的定义)，组成一个key-value的形式输出返回给reducer，交给reducer后续处理。需要特别注意的是: mapper一次只读取一行(或者说一个记录)，也就是说在mapper中的代码逻辑是为了处理一行(一条记录)的，MapReduce本身会自动循环多次，从而达到处理所有数据的效果。(处理所有读入的行的效果)
mapper返回的是  (key1,value1)，返回给reducer，这时候reducer接收到一对 (key,value)形式的数据。
![image](https://user-images.githubusercontent.com/116338825/197402654-5ddc783c-d693-409d-b974-8ea2e1d68985.png)


2. shuffle阶段(洗牌): 这个阶段MapReduce本身已经帮使用者实现了，也就是说使用者只需要实现map和reduce即可完成整个mapreduce过程逻辑，而不需要去开发shuffle程序的逻辑。shuffle实现的是：在map返回的所有数据里，把key相同的放到一个组里面(可以把这个组理解成一个类似数组的东西(可迭代对象))，于是，当map返回的各个单条数据组成的全部数据里有多个不同的key的时候，shuffle里面就会有多个组。这些个组将会一个一个的给reducer处理！当然，map处理过一行数据之后并不会等待其它行的数据也处理完之后才传给shuffle、让shuffle处理划分成组；而是map处理一条数据之后将结果返回给shuffle，这时候shuffle里面之后第一条数据，这条数据达到类似tensorflow1.0里面的占位的作用，当map处理完下一条或者其它条的时候，map会继续将结果传递给shuffle，只要传进去的(key,value)在shuffle中已经有了，它就会归到那个已经有占位的和它有相同的key的组，如果传进去的(key,value)在shuffle中没有，那它将继续占位；直到所有map工作任务达到100%。也就是说在map工作的时候shuffle也是在工作的。
简单点概括shuffle原理: 在map和reduce阶段进行排序时，比较的是k2。v2是不参与排序比较的。如果要想让v2也进行排序，需要把k2和v2组装成新的类，作为k2，才能参与比较。分组时也是按照k2进行比较的。自定义排序规则需要实现WritableComparable接口，重写compareTo方法。

3. reducer阶段:
reducer接收到来自shuffle的一组一组的数据，这时候reducer对其进行统计的操作即可达到统计key的个数的效果。当然这看起来所有过程都是在进行统计的过程，但是如果在其中加入一些算法的计算，那么MapReduce将以其强大的集群算力帮我实现大数据集的计算。

4. MapReduce模型过程要点概括；
任务Job = Map + Reduce
Map的输出是Reduce的输入
所有的输入和输出都是<Key,Value>形式：
<k1,v1>是Map的输入，<k2,v2>是Map的输出；
<k3,v3>是Reduce的输入，<k4,v4>是Reduce的输出；
k2=k3，v3是一个集合，v3的元素就是v2
所有的输入和输出的数据类型必须是hadoop的数据类型（实现Writable接口）
MapReduce处理的数据都是HDFS的数据（或HBase）



### 使用须知
- 作为学习使用。
- 要是侵权请联系删除。
