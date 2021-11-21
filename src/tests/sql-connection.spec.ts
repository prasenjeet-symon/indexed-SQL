import { ISQLConnection } from "../lib/sql-connection";
/**
 * @jest-environment jsdom
 */
describe("index/ISQLConnection", () => {
  test("test 1", async () => {
    const query = `
        CREATE TABLE student_table
        (
            student_id bigint unsigned primary key auto_increment,
            full_name varchar(233),
            date_of_birth date,
            gender enum('male', 'female', 'other') default 'male',
            date_created timestamp
        );

        INSERT INTO student_table  (  full_name , date_of_birth )
        VALUES
        ( 'Prasenjeet Symon', '1999-02-14' ),
        ( 'Divya Devshree', '1999-02-15'),
        ( 'Ritesh Chaurasiya', '1998-09-12');

        SELECT 
        full_name, 
        date_of_birth,
        gender,
        date_created

        FROM student_table
        ORDER BY date_of_birth ASC;
        `;

    const connection_db = await new ISQLConnection({
      database_name: "test_database",
      multi_query: true,
    }).open();
    await connection_db.query(query);
    expect(1).toBe(1);
  });
});
