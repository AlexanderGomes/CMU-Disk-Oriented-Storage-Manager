
      CREATE TABLE user (
	   ID INT PRIMARY KEY,
	 	Name VARCHAR(255),
	 	Age INT
      )

	   CREATE TABLE student (
	   ID INT PRIMARY KEY,
	 	Name VARCHAR(255),
	 	Age INT,
		School VARCHAR(255),
      Grade VARCHAR(2)
      )

      INSERT INTO user (ID, Name, Age) VALUES
(1, 'John', 25),
(2, 'Alice', 30),
(3, 'Bob', 28),
(4, 'Emily', 35),
(5, 'Michael', 40),
(6, 'Jane', 33),
(7, 'David', 29),
(8, 'Sarah', 27),
(9, 'Chris', 31),
(10, 'Emma', 26),
(11, 'James', 29),
(12, 'Olivia', 32),
(13, 'William', 34),
(14, 'Sophia', 27),
(15, 'Daniel', 39),
(16, 'Ava', 24),
(17, 'Matthew', 36),
(18, 'Ella', 28),
(19, 'Andrew', 37),
(20, 'Mia', 25);


INSERT INTO student (ID, Name, Age, School, Grade) VALUES
(1, 'Alex', 18, 'High School A', 'A'),
(2, 'Sophie', 17, 'High School B', 'B'),
(3, 'Lucas', 16, 'High School C', 'B+'),
(4, 'Lily', 18, 'High School A', 'A-'),
(5, 'Ethan', 17, 'High School B', 'A'),
(6, 'Chloe', 16, 'High School C', 'B'),
(7, 'Noah', 18, 'High School A', 'B+'),
(8, 'Isabella', 17, 'High School B', 'A-'),
(9, 'Logan', 16, 'High School C', 'A'),
(10, 'Ava', 18, 'High School A', 'A-'),
(11, 'Mason', 17, 'High School B', 'B'),
(12, 'Mia', 16, 'High School C', 'A'),
(13, 'Liam', 18, 'High School A', 'B+'),
(14, 'Harper', 17, 'High School B', 'B'),
(15, 'Elijah', 16, 'High School C', 'A-'),
(16, 'Amelia', 18, 'High School A', 'A'),
(17, 'Aiden', 17, 'High School B', 'A-'),
(18, 'Abigail', 16, 'High School C', 'B+'),
(19, 'Benjamin', 18, 'High School A', 'B'),
(20, 'Charlotte', 17, 'High School B', 'A');

   SELECT Name, Age 
	FROM user 
	JOIN student ON user.Name = student.Name;	