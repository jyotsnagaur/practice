CREATE TABLE Users1 (
    SNo INT PRIMARY KEY,
	UserID VARCHAR,
    company VARCHAR(50),
    start INT,
    fin INT
);

INSERT INTO Users1 (SNo, UserID, company, start, fin) VALUES
(1,'E1', 'A', 2015, 2018),
(2,'E2', 'B', 2014, 2019),
(3,'E1', 'C', 2018, 2024),
(4,'E2', 'D', 2019, 2024);

SELECT * FROM Users1

select A.UserID
from Users1 as A
join Users1 as B on A.UserID=B.UserID
where 
A.company='A'
and B.company='C';



