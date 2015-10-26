# mapreduce设计模式解析

---

### 引言
- mapreduce的设计模式多种多样，本文加以总结；
- 每种设计模式都配有具体的例子和代码；

### 设计模式

- 分类（classfication）：[统计一个网站有哪些用户访问](https://github.com/changsiyuan/mapreduce_design_pattern/blob/master/classfication.java)
- 计数（counting）：[wordcount](https://github.com/changsiyuan/mapreduce_design_pattern/blob/master/counting/wordcount%E7%A8%8B%E5%BA%8F.java)、[统计每个用户的总流量](https://github.com/changsiyuan/mapreduce_design_pattern/blob/master/counting/%E7%BB%9F%E8%AE%A1%E6%AF%8F%E4%B8%AA%E7%94%A8%E6%88%B7%E7%9A%84%E6%80%BB%E6%B5%81%E9%87%8F.java)
- 过滤（filtering）：[将记录中属性为0的过滤掉](https://github.com/changsiyuan/mapreduce_design_pattern/tree/master/filtering/%E5%B0%86%E8%AE%B0%E5%BD%95%E4%B8%AD%E5%B1%9E%E6%80%A7%E4%B8%BA0%E7%9A%84%E8%BF%87%E6%BB%A4%E6%8E%89)
- 排序（sorting）：[统计上下行流量总和并排序输出](https://github.com/changsiyuan/mapreduce_design_pattern/blob/master/sorting/%E7%BB%9F%E8%AE%A1%E4%B8%8A%E4%B8%8B%E8%A1%8C%E6%B5%81%E9%87%8F%E6%80%BB%E5%92%8C%E5%B9%B6%E6%8E%92%E5%BA%8F%E8%BE%93%E5%87%BA.java)
- 相关计数（Cross-Correlation）：[统计每个用户访问每个网址的次数](https://github.com/changsiyuan/mapreduce_design_pattern/tree/master/Cross-Correlation/%E7%BB%9F%E8%AE%A1%E6%AF%8F%E4%B8%AA%E7%94%A8%E6%88%B7%E8%AE%BF%E9%97%AE%E6%AF%8F%E4%B8%AA%E7%BD%91%E5%9D%80%E7%9A%84%E6%AC%A1%E6%95%B0)
- 去重计数（Distinct Counting）：[统计每个网址被几个手机号访问](https://github.com/changsiyuan/mapreduce_design_pattern/blob/master/Distinct%20Counting/%E7%BB%9F%E8%AE%A1%E6%AF%8F%E4%B8%AA%E7%BD%91%E5%9D%80%E8%A2%AB%E5%87%A0%E4%B8%AA%E6%89%8B%E6%9C%BA%E5%8F%B7%E8%AE%BF%E9%97%AE/%E7%BB%9F%E8%AE%A1%E6%AF%8F%E4%B8%AA%E7%BD%91%E5%9D%80%E8%A2%AB%E5%87%A0%E4%B8%AA%E6%89%8B%E6%9C%BA%E5%8F%B7%E8%AE%BF%E9%97%AE.java)
