select "TO_CHAR"(U_DATE, 'MM-dd') as U_DATE, sum(cases) as cases, sum(deaths) as deaths
FROM USA_ZONG
where (
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-01-25' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-01-30' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-02-05' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-02-10' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-02-15' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-02-20' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-02-25' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-01' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-05' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-10' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-15' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-20' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-25' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-03-30' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-04-05' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-04-10' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-04-15' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-04-20' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-04-25' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-01' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-05' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-10' or
        "TO_CHAR"(U_DATE, 'yyyy-MM-dd') = '2020-05-16'
    )
  and state = 'New York'
GROUP BY U_DATE
ORDER BY U_DATE;