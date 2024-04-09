INSERT INTO student (ID, Name, Age) 
	VALUES 
		(1, 'John Doe', 30, "pinole"),
		(2, 'Jane Smith', 25, "hercules"),
		(3, 'Michael Johnson', 40, "pinole"),
		(4, 'Emily Davis', 35, "hercules"),
		(5, 'Christopher Wilson', 28, "pinole"),
		(6, 'Jessica Martinez', 33, "pinole"),
		(7, 'Daniel Brown', 45, "pinole"),
	

      CREATE TABLE user (
	ID INT PRIMARY KEY,
	 	Name VARCHAR(255),
	 	Age INT
      )

	   CREATE TABLE student (
	    ID INT PRIMARY KEY,
	 	Name VARCHAR(255),
	 	Age INT,
		School VARCHAR(255)
      )

	  SELECT name, age 
	FROM user 
	JOIN student ON user.name = student.name;	