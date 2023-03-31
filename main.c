#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "pager.h"
#include "table.h"
#include "database.h"
#include "utils.h"
#include "query.h"

void test_scheme_addition(){

    database* db = create_database("db.ldb", 4096);
    page* db_pg = allocate_page(db->block_size);
    write_database_to_page(db, db_pg, sizeof(block));

    FILE* f = fopen("db.ldb", "wb+");
    write_page_to_file(f, db_pg, 0);
    database* db2 = read_database_from_page(db_pg, sizeof(block));
    initialize_schema(db, f);

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col3 = create_column("height", FLOAT,sizeof(float));
    column* col4 = create_column("active", BOOL,sizeof(bool));
    column** cols = create_columns(col1, col2, col3, col4);

    table* tb = create_table("studs", 4, cols);

    for (int i = 0; i < 15; i++) {
        struct timeval start = getCurrentTime();
        for (int k = 0; k < 37; k++) {
            insert_table_to_schema(tb, db, f);
        }
        struct timeval end = getCurrentTime();
        float elapsed_time = time_diff_microseconds(start, end);
        printf("Elapsed time: %f microseconds.\n", elapsed_time);
    }

    fclose(f);

}

void check_scheme_return() {

    database* db = create_database("db.ldb", 4096);
    page* db_pg = allocate_page(4096);
    write_database_to_page(db, db_pg, sizeof(block));

    FILE* f = fopen("db.ldb", "wb+");
    write_page_to_file(f, db_pg, 0);
    initialize_schema(db, f);

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col3 = create_column("height", FLOAT,sizeof(float));
    column* col4 = create_column("active", BOOL,sizeof(bool));
    column** cols = create_columns(col1, col2, col3, col4);

    char name[118];

    int k = 0;
    while (k < 37) {
        sprintf(name, "tb%d", k);
        table* tb = create_table(name, 4, cols);
        insert_table_to_schema(tb, db, f);
        free(tb);
        k++;
    }
    k = 0;
    while (k < 37) {
        sprintf(name, "tb%d", k);
        table* tb = get_table_from_schema(name, db, f);
        printf("Table: \"name\" :  \"%s\"\n", tb->name);
        free(tb);
        k++;
    }

    fclose(f);

}

void deletion_from_scheme() {
    database* db = create_database("db.ldb", 4096);
    page* db_pg = allocate_page(4096);
    write_database_to_page(db, db_pg, sizeof(block));

    FILE* f = fopen("db.ldb", "wb+");
    write_page_to_file(f, db_pg, 0);
    initialize_schema(db, f);

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col3 = create_column("height", FLOAT,sizeof(float));
    column* col4 = create_column("active", BOOL,sizeof(bool));
    column* col5 = create_column("age", INTEGER,sizeof(int32_t));

    column** cols1 = create_columns(col1, col2, col3, col4, col5);
    column** cols2 = create_columns(col1, col2, col4);


    table *tb1 = create_table("tb1", 5, cols1);
    insert_table_to_schema(tb1, db, f);
    initialize_table_record_block(tb1, db, f);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f);
    initialize_table_record_block(tb2, db, f);

    char* n[4] = {"id", "name", "active", "height"};

    table_to_join* left = create_table_to_join(tb1, 4, n, NULL, col1);
    table_to_join* right = create_table_to_join(tb2, 3, n, NULL, col1);



    for (int i = 0; i < 10; i++) {

        record *r = create_record(tb1);
        insert_integer_record(tb1, r, "id", i);
        insert_string_record(tb1, r, "name", "Stefan");
        insert_float_record(tb1, r, "height", 100.5f);
        insert_boolean_record(tb1, r, "active", true);
        insert_integer_record(tb1, r, "age", 18);

        insert_record_to_table(r, tb1, db, f);
        destroy_record(r);
    }

    for (int j = 0; j < 10; j++) {

        record *r = create_record(tb2);
        insert_integer_record(tb2, r, "id", j);
        insert_string_record(tb2, r, "name", "Bogdan");
        insert_boolean_record(tb2, r, "active", false);

        insert_record_to_table(r, tb2, db, f);
        destroy_record(r);
    }

    fclose(f);
}

int main() {

    database* db = create_database("db.ldb", 4096);
    page* db_pg = allocate_page(4096);
    write_database_to_page(db, db_pg, sizeof(block));

    FILE* f = fopen("db.ldb", "wb+");
    write_page_to_file(f, db_pg, 0);
    initialize_schema(db, f);

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col3 = create_column("height", FLOAT,sizeof(float));
    column* col4 = create_column("active", BOOL,sizeof(bool));
    column* col5 = create_column("age", INTEGER,sizeof(int32_t));

    column* cols1[5] = {col1, col2, col3, col4, col5};
    column** cols2 = create_columns(col1, col2, col4);

    table *tb1 = create_table("tb1", 5, cols1);
    insert_table_to_schema(tb1, db, f);
    initialize_table_record_block(tb1, db, f);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f);
    initialize_table_record_block(tb2, db, f);

    char* n[5] = {"id", "name", "active", "height", "age"};


    for (int i = 0; i < 1000; i++) {

        record *r = create_record(tb1);
        insert_integer_record(tb1, r, "id", i);
        insert_string_record(tb1, r, "name", "Stefan");
        insert_float_record(tb1, r, "height", 100.5f);
        insert_boolean_record(tb1, r, "active", true);
        insert_integer_record(tb1, r, "age", 18);


        insert_record_to_table(r, tb1, db, f);
        destroy_record(r);
    }
    for (int j = 0; j < 100; j++) {

        record *r = create_record(tb2);
        insert_integer_record(tb2, r, "id", j);
        insert_string_record(tb2, r, "name", "Bogdan");
        insert_boolean_record(tb2, r, "active", false);

        insert_record_to_table(r, tb2, db, f);
        destroy_record(r);
    }

    uint32_t left_off = 0;
    uint32_t right_off = 0;
    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));

    condition_operand* lco = create_condition_string_operand("id");
    condition_operand* rco = create_condition_string_operand("age");
    condition* ic = create_condition_join(LEFT,COLUMN_COLUMN, lco, rco, LESS_EQUAL, NONE);

    condition_operand* lco2 = create_condition_string_operand("id");
    condition_operand* rco2 = create_condition_integer_operand(10);
    condition* ic2 = create_condition_join(RIGHT,COLUMN_VALUE, lco2, rco2, MORE_EQUAL, NONE);

    condition_operand* lco3 = create_condition_string_operand("name");
    condition_operand* rco3 = create_condition_string_operand("Stefa");
    condition* sc = create_condition_join(LEFT, COLUMN_VALUE, lco3, rco3, LIKE, NONE);

    //ic->next = ic2;


    table* tb = get_table_from_schema("tb1", db, f);

    //column_to_update* colcc = create_integer_column_to_update("id", 5);
    //update_records_in_table(colcc, ic, tb, db, f);
    //delete_records_from_table(ic, tb, db, f);

    table_to_join* left = create_table_to_join(tb, 4, n, NULL, col1);
    table_to_join* right = create_table_to_join(tb2, 3, n, NULL, col1);

    do {
        select_records_from_table_inner_join(&left_off, &right_off,buffer, 1024,left, right, sc,db, f);
        //left_off = select_records_from_table(left_off, buffer, 1024, 5, n, ic2,tb, db, f);
        printf("%s", buffer);
        memset(buffer, 0, sizeof(buffer));
    } while(left_off != 0);



    free(tb1);
    free(db->blk);
    free(db);
    destroy_page(db_pg);
    free(col1);
    free(col2);
    free(col3);
    free(col4);
    free(cols2);
    fclose(f);

    return 0;

}

void insert_test(database* db, FILE* f) {

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col4 = create_column("active", BOOL,sizeof(bool));

    column** cols2 = create_columns(col1, col2, col4);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f);
    initialize_table_record_block(tb2, db, f);

    record *r = create_record(tb2);
    insert_integer_record(tb2, r, "id", 1);
    insert_string_record(tb2, r, "name", "Bogdan");
    insert_boolean_record(tb2, r, "active", false);

    for (int i = 0; i < 100; ++i) {
        struct timeval start = getCurrentTime();

        for (int j = 0; j < 10000; j++) {

            insert_record_to_table(r, tb2, db, f);

        }

        struct timeval end = getCurrentTime();
        float elapsed_time = time_diff_microseconds(start, end);
        printf("%f\n", elapsed_time);

    }
}

void select_test(database* db, FILE* f_in, FILE* f_out) {

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col4 = create_column("active", BOOL,sizeof(bool));

    column** cols2 = create_columns(col1, col2, col4);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f_in);
    initialize_table_record_block(tb2, db, f_in);

    record *r = create_record(tb2);
    insert_integer_record(tb2, r, "id", 1);
    insert_string_record(tb2, r, "name", "Bogdan");
    insert_boolean_record(tb2, r, "active", false);

    char* n[4] = {"id", "name", "active"};

    for (int i = 0; i < 100; ++i) {

        for (int j = 0; j < 10000; j++) {

            insert_record_to_table(r, tb2, db, f_in);

        }

        struct timeval start = getCurrentTime();

        //select_records_from_table(3, n, NULL, tb2, db, f_in, f_out);

        struct timeval end = getCurrentTime();
        float elapsed_time = time_diff_microseconds(start, end);
        printf("%f\n", elapsed_time);

    }
}

void delete_test(database* db, FILE* f_in) {

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col4 = create_column("active", BOOL,sizeof(bool));

    column** cols2 = create_columns(col1, col2, col4);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f_in);
    initialize_table_record_block(tb2, db, f_in);

    record *r = create_record(tb2);
    insert_integer_record(tb2, r, "id", 1);
    insert_string_record(tb2, r, "name", "Bogdan");
    insert_boolean_record(tb2, r, "active", false);

    char* n[4] = {"id", "name", "active"};

    for (int i = 0; i < 100; ++i) {

        for (int j = 0; j < 10000; j++) {

            insert_record_to_table(r, tb2, db, f_in);

        }

        struct timeval start = getCurrentTime();

        //condition* ic1 = create_integer_condition("id", MORE, i*5000, NONE);
        //condition* ic2 = create_integer_condition("id", LESS_EQUAL, i*5000 + 5000, NONE);
        //ic1->next = ic2;

        //delete_records_from_table(ic1, tb2, db, f_in);

        struct timeval end = getCurrentTime();
        float elapsed_time = time_diff_microseconds(start, end);
        printf("%f\n", elapsed_time);

    }
}

void update_test(database* db, FILE* f_in) {

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col4 = create_column("active", BOOL,sizeof(bool));

    column** cols2 = create_columns(col1, col2, col4);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f_in);
    initialize_table_record_block(tb2, db, f_in);

    record *r = create_record(tb2);
    insert_integer_record(tb2, r, "id", 1);
    insert_string_record(tb2, r, "name", "Bogdan");
    insert_boolean_record(tb2, r, "active", false);

    column_to_update* cu = create_integer_column_to_update("id", 0);

    for (int i = 0; i < 100; ++i) {

        for (int j = 0; j < 10000; j++) {

            insert_record_to_table(r, tb2, db, f_in);

        }

        struct timeval start = getCurrentTime();

        //condition* ic1 = create_integer_condition("id", MORE, i*5000, NONE);
        //condition* ic2 = create_integer_condition("id", LESS_EQUAL, i*5000 + 5000, NONE);
        //ic1->next = ic2;

        //update_records_in_table(cu, ic1, tb2, db, f_in);

        struct timeval end = getCurrentTime();
        float elapsed_time = time_diff_microseconds(start, end);
        printf("%f\n", elapsed_time);

    }
}

void join_test(database* db, FILE* f_in, FILE* f_out) {
    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col3 = create_column("height", FLOAT,sizeof(float));
    column* col4 = create_column("active", BOOL,sizeof(bool));

    column** cols1 = create_columns(col1, col2, col3, col4);
    column** cols2 = create_columns(col1, col2, col4);


    table *tb1 = create_table("tb1", 4, cols1);
    insert_table_to_schema(tb1, db, f_in);
    initialize_table_record_block(tb1, db, f_in);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f_in);
    initialize_table_record_block(tb2, db, f_in);

    char* n[4] = {"id", "name", "active", "height"};

    table_to_join* left = create_table_to_join(tb1, 4, n, NULL, col1);
    table_to_join* right = create_table_to_join(tb2, 3, n, NULL, col1);


    int k = 0;
    for (int i = 0; i < 50; ++i) {

        for (int j = 0; j < 50; j++) {

            record *r = create_record(tb1);
            insert_integer_record(tb1, r, "id", k);
            insert_string_record(tb1, r, "name", "Stefan");
            insert_float_record(tb1, r, "height", 100.5f);
            insert_boolean_record(tb1, r, "active", true);

            insert_record_to_table(r, tb1, db, f_in);
            destroy_record(r);
            k++;
        }
        k-=50;
        for (int j = 0; j < 100; j++) {

            record *r = create_record(tb2);
            insert_integer_record(tb2, r, "id", k);
            insert_string_record(tb2, r, "name", "Bogdan");
            insert_boolean_record(tb2, r, "active", false);

            insert_record_to_table(r, tb2, db, f_in);
            destroy_record(r);
            k++;
        }

        struct timeval start = getCurrentTime();

        //select_records_from_table_inner_join(left, right, db, f_in, f_out);

        struct timeval end = getCurrentTime();
        float elapsed_time = time_diff_microseconds(start, end);
        printf("%f\n", elapsed_time);

    }
}

void testing_creation_insertion_deletion_update_select_time() {
    database* db = create_database("db.ldb", 4096);
    page* db_pg = allocate_page(4096);
    write_database_to_page(db, db_pg, sizeof(block));

    FILE* f = fopen("db.ldb", "wb+");
    write_page_to_file(f, db_pg, 0);
    initialize_schema(db, f);

    column* col1 = create_column("id", INTEGER,sizeof(int32_t));
    column* col2 = create_column("name", STRING,MAX_NAME_LENGTH);
    column* col3 = create_column("height", FLOAT,sizeof(float));
    column* col4 = create_column("active", BOOL,sizeof(bool));

    column** cols1 = create_columns(col1, col2, col3, col4);
    column** cols2 = create_columns(col1, col2, col4);


    table *tb1 = create_table("tb1", 4, cols1);
    insert_table_to_schema(tb1, db, f);
    initialize_table_record_block(tb1, db, f);

    initialize_first_free_block(db, f);

    table *tb2 = create_table("tb2", 3, cols2);
    insert_table_to_schema(tb2, db, f);
    initialize_table_record_block(tb2, db, f);

    uint32_t k = 0;
    for(int i = 0; i < 1000; i++) {
        struct timeval start = getCurrentTime();
        for (int j = 0; j < 1000; j++) {

            record *r = create_record(tb1);
            insert_integer_record(tb1, r, "id", k);
            insert_string_record(tb1, r, "name", "Stefan");
            insert_float_record(tb1, r, "height", 100.5f);
            insert_boolean_record(tb1, r, "active", true);

            insert_record_to_table(r, tb1, db, f);
            destroy_record(r);
            k++;
        }
        //condition* c = create_integer_condition("id", EQUAL, k);
        //column_to_update* cu = create_integer_column_to_update("id", 5);

        //struct timeval start = getCurrentTime();
        //char* n[4] = {"id", "name", "height", "active"};
        //select_records_from_table(4, n,NULL,tb1, db, f);
        //delete_records_from_table(c, tb1, db, f);
        //update_records_in_table(cu, c, tb1, db, f);
        struct timeval end = getCurrentTime();
        long long elapsed_time = time_diff_microseconds(start, end);
        printf("%lld\n", elapsed_time);

    }
    //select_records_from_table(4, n,NULL,tb1, db, f);



    fclose(f);
}
