'Deepak.Kr.Baran@gmail.com'
===========================
select fn ,mn, dn,rst,ln from 
( --2
select fn,substr(rst,1,instr(rst,'.')-1) mn, dn,rst,
(SELECT SUBSTR(rst, INSTR(rst, '.') + 1, INSTR(rst, '@') - INSTR(rst, '.') - 1) from dual )AS ln
 from  
(--1
SELECT     SUBSTR('Deepak.Kr.Baran@gmail.com', 1, INSTR('Deepak.Kr.Baran@gmail.com', '.') - 1) AS fn,
          ( select  SUBSTR('Deepak.Kr.Baran@gmail.com', INSTR('Deepak.Kr.Baran@gmail.com', '@') +1) from dual ) as dn,
          ( select  SUBSTR('Deepak.Kr.Baran@gmail.com', INSTR('Deepak.Kr.Baran@gmail.com', '.')+1 ) stg from dual) as rst
          from dual
) --1        
)--2      
;