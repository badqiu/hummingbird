select name,test_fun(name) name_part,
case
when sva=1 then '男' 
else '女' 
end sex ,
from taname 
where sva = ''