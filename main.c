#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "pager.h"
#include "table.h"
#include "database.h"
#include "utils.h"
#include "query.h"

void check_scheme_addition(){

    database* db = create_database("db.ldb", 4096);
    page* db_pg = allocate_page(4096);
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
        long long elapsed_time = timediff_microseconds(start, end);
        printf("Elapsed time: %ld microseconds.\n", elapsed_time);
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
    column** cols = create_columns(col1, col2, col3, col4);

    char name[118];

    for (int i = 0; i < 25; i++) {
        int k = 0;
        while (k < 10000) {
            sprintf(name, "tb%d%d", k, i);
            table* tb = create_table(name, 4, cols);
            insert_table_to_schema(tb, db, f);

            free(tb);
            k++;

        }
        struct timeval start = getCurrentTime();

        sprintf(name, "tb%d%d", 10000, i);
        delete_table_from_schema(name, db, f);

        struct timeval end = getCurrentTime();
        long long elapsed_time = timediff_microseconds(start, end);
        printf("Elapsed time: %ld microseconds.\n", elapsed_time);
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

    column** cols1 = create_columns(col1, col2, col3, col4);
    column** cols2 = create_columns(col1, col2, col4);


    table *tb1 = create_table("tb1", 4, cols1);
    insert_table_to_schema(tb1, db, f);
    initialize_table_record_block(tb1, db, f);

    uint32_t k = 0;
    for (int j = 0; j < 100000; j++) {

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

    delete_records_from_table(NULL, tb1, db, f);

    for (int j = 0; j < 100000; j++) {

        record *r = create_record(tb1);
        insert_integer_record(tb1, r, "id", k);
        insert_string_record(tb1, r, "name", "Stefan");
        insert_float_record(tb1, r, "height", 100.5f);
        insert_boolean_record(tb1, r, "active", true);

        insert_record_to_table(r, tb1, db, f);
        destroy_record(r);
        k++;
    }



    fclose(f);
    return 0;

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
        long long elapsed_time = timediff_microseconds(start, end);
        printf("%ld\n", elapsed_time);

    }
    //select_records_from_table(4, n,NULL,tb1, db, f);



    fclose(f);
}
