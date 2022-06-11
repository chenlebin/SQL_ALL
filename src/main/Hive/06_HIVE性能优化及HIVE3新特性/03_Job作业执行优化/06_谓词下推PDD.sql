--todo 谓词下推（PPD）
--1>谓词：用来描述或判定客体性质、特征或者客体之间关系的词项。比如"3 大于 2"中"大于"是一个谓词。
--2>谓词下推Predicate Pushdown（PPD）基本思想：将过滤表达式尽可能移动至靠近数据源的位置，
--以使真正执行时能直接跳过无关的数据。简单点说就是在不影响最终结果的情况下，尽量将过滤条件提前执行。
--3>Hive中谓词下推后，过滤条件会下推到map端，提前执行过滤，减少map到reduce的传输数据，提升整体性能。
--todo 开启参数【默认开启】
set hive.optimize.ppd=true;

--方式一 先进行过滤在进行join提前执行过滤进行谓词下推
select a.id, a.value1, b.value2
from table1 a
         join (select b.* from table2 b where b.ds >= '20181201' and b.ds < '20190101') c
              on (a.id = c.id);

--方式二 最后才进行过滤是整体过滤已经join完了，性能较低
select a.id, a.value1, b.value2
from table1 a
         join table2 b on a.id = b.id
where b.ds >= '20181201'
  and b.ds < '20190101';

--todo 规则
//1、对于Join(Inner Join)、Full outer Join，条件写在on后面，还是where后面，性能上面没有区别；
//2、对于Left outer Join ，右侧的表写在on后面、左侧的表写在where后面，性能上有提高；
//3、对于Right outer Join，左侧的表写在on后面、右侧的表写在where后面，性能上有提高；
//4、当条件分散在两个表时，谓词下推可按上述结论2和3自由组合。
