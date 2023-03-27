//
// Created by Stefan on 16/03/2023.
//

#include "query.h"
#include <stdlib.h>
#include <string.h>

table_to_join* create_table_to_join(table* tb,
                                    uint8_t num_cols,
                                    char** view_cols,
                                    condition* cond,
                                    column* col_to_join){
    table_to_join* tj = malloc(sizeof(table_to_join));
    tj->tb = tb;
    tj->num_cols = num_cols;
    tj->view_cols = view_cols;
    tj->cond = cond;
    tj->col_to_join = col_to_join;
}

condition* create_integer_condition(char* name, enum relation relation, int32_t val){
    condition* c = calloc(1, sizeof(condition));
    c->col_name = name;
    c->type = INTEGER;
    c->size = sizeof(int32_t);
    c->relation = relation;
    c->value = calloc(1, c->size);
    c->value->i = val;
    c->next = NULL;
    return c;
}

condition* create_float_condition(char* name, enum relation relation, float val){
    condition* c = calloc(1, sizeof(condition));
    c->col_name = name;
    c->type = FLOAT;
    c->size = sizeof(float);
    c->relation = relation;
    c->value = calloc(1, c->size);
    c->value->f = val;
    c->next = NULL;
    return c;
}

condition* create_bool_condition(char* name, enum relation relation, bool val){
    condition* c = calloc(1, sizeof(condition));
    c->col_name = name;
    c->type = BOOL;
    c->size = sizeof(bool);
    c->relation = relation;
    c->value = calloc(1, c->size);
    c->value->b = val;
    c->next = NULL;
    return c;
}

condition* create_string_condition(char* name, enum relation relation, char* val){
    condition* c = calloc(1, sizeof(condition));
    c->col_name = name;
    c->type = STRING;
    c->size = strlen(val) + 1;
    c->relation = relation;
    c->value = calloc(1, c->size);
    c->value->s = val;
    c->next = NULL;
    return c;
}

column_to_update* create_integer_column_to_update(char* name, int32_t val) {

    column_to_update* c = calloc(1, sizeof(column_to_update));
    c->col_name = name;
    c->type = INTEGER;
    c->size = sizeof(int32_t);
    c->value = calloc(1, c->size);
    c->value->i = val;
    c->next = NULL;
    return c;
}

column_to_update* create_float_column_to_update(char* name, float val) {

    column_to_update* c = calloc(1, sizeof(column_to_update));
    c->col_name = name;
    c->type = FLOAT;
    c->size = sizeof(float);
    c->value = calloc(1, c->size);
    c->value->f = val;
    c->next = NULL;
    return c;
}

column_to_update* create_bool_column_to_update(char* name, bool val) {

    column_to_update* c = calloc(1, sizeof(column_to_update));
    c->col_name = name;
    c->type = BOOL;
    c->size = sizeof(bool);
    c->value = calloc(1, c->size);
    c->value->b = val;
    c->next = NULL;
    return c;
}

column_to_update* create_string_column_to_update(char* name, char* val) {

    column_to_update* c = calloc(1, sizeof(column_to_update));
    c->col_name = name;
    c->type = STRING;
    c->size = strlen(val) + 1;
    c->value = calloc(1, c->size);
    strcpy(c->value->s, val);
    c->next = NULL;
    return c;
}


bool check_integers(int32_t first, int32_t second, enum relation rel){
    switch (rel) {
        case LESS:
            if (first < second){
                return true;
            }
            break;
        case LESS_EQUAL:
            if (first <= second){
                return true;
            }
            break;
        case MORE:
            if (first > second){
                return true;
            }
            break;
        case MORE_EQUAL:
            if (first >= second){
                return true;
            }
            break;
        case EQUAL:
            if (first == second){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (first != second){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

bool check_floats(float first, float second, enum relation rel){
    switch (rel) {
        case LESS:
            if (first < second){
                return true;
            }
            break;
        case LESS_EQUAL:
            if (first <= second){
                return true;
            }
            break;
        case MORE:
            if (first > second){
                return true;
            }
            break;
        case MORE_EQUAL:
            if (first >= second){
                return true;
            }
            break;
        case EQUAL:
            if (first == second){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (first != second){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

bool check_bools(bool first, bool second, enum relation rel){
    switch (rel) {
        case EQUAL:
            if (first == second){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (first != second){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

bool check_strings(char* first, char* second, enum relation rel){
    switch (rel) {
        case LESS:
            if (strlen(first) < strlen(second)){
                return true;
            }
            break;
        case LESS_EQUAL:
            if (strlen(first) <= strlen(second)){
                return true;
            }
            break;
        case MORE:
            if (strlen(first) > strlen(second)){
                return true;
            }
            break;
        case MORE_EQUAL:
            if (strlen(first) >= strlen(second)){
                return true;
            }
            break;
        case EQUAL:
            if (!strcmp(first, second)){
                return true;
            }
            break;
        case NOT_EQUAL:
            if (strcmp(first, second)){
                return true;
            }
            break;
        default:
            return false;
    }
    return false;
}

uint32_t select_records_from_table(uint32_t block_offset, char* buffer, uint32_t buff_sz, uint8_t num_cols, char** view_cols,
                                   condition* cond, table* tb, database* db, FILE* f) {
    uint32_t buffer_offset = 0;
    page *curr_pg = allocate_page(db->block_size);
    uint32_t off = sizeof(block);
    if(block_offset==0) {
        read_page_from_file(f, curr_pg, tb->first_record_block_id);
    } else {
        read_page_from_file(f, curr_pg, block_offset/db->block_size);
        off = block_offset % db->block_size;
    }

    block *curr_blk = read_block_from_page(curr_pg);

    char* string = calloc(1, 2);
    while((curr_blk->id != tb->last_record_block_id) || (off < curr_blk->offset)){
        if(off >= curr_blk->offset){
            off = sizeof(block);
            get_next_block(curr_blk->next_id, curr_blk, curr_pg, f);
        }
        record* r = read_record_from_page(tb, curr_pg, off);
        off += get_columns_size(tb) + sizeof(bool);

        memcpy(string, "|", 2);
        for (int i = 0; i < num_cols; i++) {
            column* c = get_column_by_name(tb, view_cols[i]);
            switch (c->type) {
                case INTEGER:
                    string = integer_to_string(get_integer_record(tb, r, c->name), string);
                    break;
                case FLOAT:
                    string = float_to_string(get_float_record(tb, r, c->name), string);
                    break;
                case BOOL:
                    string = bool_to_string(get_boolean_record(tb, r, c->name), string);
                    break;
                case STRING:;
                    char* temp = get_string_record(tb, r, c->name);
                    string = string_to_string(temp, string);
                    free(temp);
                    break;
            }
        }

        bool flag = true;
        condition* temp = cond;
        while(temp != NULL) {

            switch (temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(tb, r, temp->col_name);
                    if(!check_integers(i_n, temp->value->i, temp->relation)){
                        flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(tb, r, temp->col_name);
                    if(!check_floats(f_n, temp->value->f, temp->relation)){
                        flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(tb, r, temp->col_name);
                    if(!check_bools(b_n, temp->value->b, temp->relation)){
                        flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(tb, r, temp->col_name);
                    if(!check_strings(s, temp->value->s, temp->relation)){
                        flag = false;
                    }
                    break;
            }

            if(!flag) {
                break;
            }
            temp = temp->next;
        }

        if(flag) {
            if(buffer_offset < buff_sz){
                memcpy(buffer+buffer_offset, string, strlen(string));
                buffer_offset += strlen(string);
                memcpy(buffer + buffer_offset, "\n", strlen("\n"));
                buffer_offset += strlen("\n");
                if(strlen(string) + 1 + buffer_offset >= buff_sz){
                    memcpy(buffer + buffer_offset, "\0", strlen("\0"));
                    return off + curr_blk->id * db->block_size;
                }
            }
        }
        destroy_record(r);
    }
    free(string);
    free(curr_blk);
    destroy_page(curr_pg);
    return 0;
}

void select_records_from_table_inner_join(uint32_t* left_block_off, uint32_t* right_block_off, char* buffer, uint32_t buff_sz,
                                          table_to_join* left, table_to_join* right, database* db, FILE* f) {
    uint32_t buffer_offset = 0;

    table* left_table = left->tb;
    page* left_pg = allocate_page(db->block_size);
    uint32_t left_off = sizeof(block);
    if(*left_block_off==0) {
        read_page_from_file(f, left_pg, left_table->first_record_block_id);
    } else {
        read_page_from_file(f, left_pg, *left_block_off/db->block_size);
        left_off = *left_block_off % db->block_size;
    }
    block* left_blk = read_block_from_page(left_pg);


    table* right_table = right->tb;
    page* right_pg = allocate_page(db->block_size);

    char* string = calloc(1, 2);

    while((left_blk->id != left_table->last_record_block_id) || (left_off < left_blk->offset)){
        if(left_off >= left_blk->offset){
            left_off = sizeof(block);
            get_next_block(left_blk->next_id, left_blk, left_pg, f);
        }
        record* left_r = read_record_from_page(left_table, left_pg, left_off);
        left_off += get_columns_size(left_table) + sizeof(bool);

        memcpy(string, "|", 2);
        for (int i = 0; i < left->num_cols; i++) {
            column* c = get_column_by_name(left_table, left->view_cols[i]);
            switch (c->type) {
                case INTEGER:
                    string = integer_to_string(get_integer_record(left_table, left_r, c->name), string);
                    break;
                case FLOAT:
                    string = float_to_string(get_float_record(left_table, left_r, c->name), string);
                    break;
                case BOOL:
                    string = bool_to_string(get_boolean_record(left_table, left_r, c->name), string);
                    break;
                case STRING:;
                    char* temp = get_string_record(left_table, left_r, c->name);
                    string = string_to_string(temp, string);
                    free(temp);
                    break;
            }
        }

        bool left_flag = true;
        condition* l_temp = left->cond;
        while(l_temp != NULL) {

            switch (l_temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(left_table, left_r, l_temp->col_name);
                    if(!check_integers(i_n, l_temp->value->i, l_temp->relation)){
                        left_flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(left_table, left_r, l_temp->col_name);
                    if(!check_floats(f_n, l_temp->value->f, l_temp->relation)){
                        left_flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(left_table, left_r, l_temp->col_name);
                    if(!check_bools(b_n, l_temp->value->b, l_temp->relation)){
                        left_flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(left_table, left_r, l_temp->col_name);
                    if(!check_strings(s, l_temp->value->s, l_temp->relation)){
                        left_flag = false;
                    }
                    break;
            }

            if(!left_flag) {
                break;
            }
            l_temp = l_temp->next;
        }

        if(left_flag) {
            char* r_string = calloc(1, 1);
            //Right table

            uint32_t right_off = sizeof(block);
            if(*right_block_off==0) {
                read_page_from_file(f, right_pg, right_table->first_record_block_id);
            } else {
                read_page_from_file(f, right_pg, *right_block_off/db->block_size);
                right_off = *right_block_off % db->block_size;
            }
            block* right_blk = read_block_from_page(right_pg);

            while((right_blk->id != right_table->last_record_block_id) || (right_off < right_blk->offset)){
                if(right_off >= right_blk->offset){
                    right_off = sizeof(block);
                    get_next_block(right_blk->next_id, right_blk, right_pg, f);
                }
                record* right_r = read_record_from_page(right_table, right_pg, right_off);
                right_off += get_columns_size(right_table) + sizeof(bool);
                memcpy(r_string, "\0", 1);
                for (int i = 0; i < right->num_cols; i++) {
                    column* c = get_column_by_name(right_table, right->view_cols[i]);
                    switch (c->type) {
                        case INTEGER:
                            r_string = integer_to_string(get_integer_record(right_table, right_r, c->name), r_string);
                            break;
                        case FLOAT:
                            r_string = float_to_string(get_float_record(right_table, right_r, c->name), r_string);
                            break;
                        case BOOL:
                            r_string = bool_to_string(get_boolean_record(right_table, right_r, c->name), r_string);
                            break;
                        case STRING:;
                            char* r_temp = get_string_record(right_table, right_r, c->name);
                            r_string = string_to_string(r_temp, r_string);
                            free(r_temp);
                            break;
                    }
                }

                bool right_flag = true;
                condition* r_temp = right->cond;
                while(r_temp != NULL) {

                    switch (r_temp->type) {
                        case INTEGER:;
                            int32_t i_n = get_integer_record(right_table, right_r, r_temp->col_name);
                            if(!check_integers(i_n, r_temp->value->i, r_temp->relation)){
                                right_flag = false;
                            }
                            break;
                        case FLOAT:;
                            float f_n = get_float_record(right_table, right_r, r_temp->col_name);
                            if(!check_floats(f_n, r_temp->value->f, r_temp->relation)){
                                right_flag = false;
                            }
                            break;
                        case BOOL:;
                            bool b_n = get_boolean_record(right_table, right_r, r_temp->col_name);
                            if(!check_bools(b_n, r_temp->value->b, r_temp->relation)){
                                right_flag = false;
                            }
                            break;
                        case STRING:;
                            char* r_s = get_string_record(right_table, right_r, r_temp->col_name);
                            if(!check_strings(r_s, r_temp->value->s, r_temp->relation)){
                                right_flag = false;
                            }
                            free(r_s);
                            break;
                    }

                    if(!right_flag) {
                        break;
                    }
                    r_temp = r_temp->next;
                }

                if(right_flag) {
                    bool join_flag = false;
                    switch (left->col_to_join->type) {
                        case INTEGER:;
                            int32_t l_i = get_integer_record(left_table, left_r, left->col_to_join->name);
                            int32_t r_i = get_integer_record(right_table, right_r, right->col_to_join->name);
                            if(check_integers(l_i, r_i, EQUAL)){
                                join_flag = true;
                            }
                            break;
                        case FLOAT:;
                            float l_f = get_float_record(left_table, left_r, left->col_to_join->name);
                            float r_f = get_float_record(right_table, right_r, right->col_to_join->name);
                            if(check_floats(l_f, r_f, EQUAL)){
                                join_flag = true;
                            }
                            break;
                        case BOOL:;
                            bool l_b = get_boolean_record(left_table, left_r, left->col_to_join->name);
                            bool r_b = get_boolean_record(right_table, right_r, right->col_to_join->name);
                            if(check_bools(l_b, r_b, EQUAL)){
                                join_flag = true;
                            }
                            break;
                        case STRING:;
                            char* l_s = get_string_record(left_table, left_r, left->col_to_join->name);
                            char* r_s = get_string_record(right_table, right_r, right->col_to_join->name);
                            if(check_strings(l_s, r_s, EQUAL)){
                                join_flag = true;
                            }
                            break;
                    }

                    if (join_flag) {
                        string = simp_str_str(r_string, string);

                        if(buffer_offset < buff_sz){
                            memcpy(buffer+buffer_offset, string, strlen(string));
                            buffer_offset += strlen(string);
                            memcpy(buffer + buffer_offset, "\n", strlen("\n"));
                            buffer_offset += strlen("\n");
                            if(strlen(string) + 1 + buffer_offset >= buff_sz){
                                memcpy(buffer + buffer_offset, "\0", strlen("\0"));
                                *left_block_off = left_off + left_blk->id * db->block_size;
                                *right_block_off = right_off + right_blk->id * db->block_size;
                                return;
                            }
                        }

                        //fprintf(fp, "%s", string);
                        //fprintf(fp, "\n");
                    }
                }
                destroy_record(right_r);
            }
            free(r_string);
            free(right_blk);
        }
        destroy_record(left_r);
    }
    *left_block_off = 0;
    free(string);
    free(left_blk);
    destroy_page(left_pg);
    destroy_page(right_pg);
}

void update_records_in_table(column_to_update* col, condition* cond, table* tb, database* db, FILE* f) {
    page* curr_pg = allocate_page(db->block_size);
    read_page_from_file(f, curr_pg, tb->first_record_block_id);
    block* curr_blk = read_block_from_page(curr_pg);

    uint32_t off = sizeof(block);

    for(uint32_t read_rows=0; read_rows < tb->num_rows; read_rows++){
        if(off >= curr_blk->offset){
            off = sizeof(block);
            get_next_block(curr_blk->next_id, curr_blk, curr_pg, f);
        }
        record* r = read_record_from_page(tb, curr_pg, off);

        bool flag = true;
        condition* temp = cond;
        while(temp != NULL) {

            switch (temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(tb, r, temp->col_name);
                    if(!check_integers(i_n, temp->value->i, temp->relation)){
                        flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(tb, r, temp->col_name);
                    if(!check_floats(f_n, temp->value->f, temp->relation)){
                        flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(tb, r, temp->col_name);
                    if(!check_bools(b_n, temp->value->b, temp->relation)){
                        flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(tb, r, temp->col_name);
                    if(!check_strings(s, temp->value->s, temp->relation)){
                        flag = false;
                    }
                    break;
            }

            if(!flag) {
                break;
            }
            temp = temp->next;
        }

        if(flag) {
            /* Update */
            column_to_update* temp_col = col;
            while(temp_col != NULL) {

                switch (temp_col->type) {
                    case INTEGER:
                        insert_integer_record(tb, r, temp_col->col_name,temp_col->value->i);
                        break;
                    case FLOAT:
                        insert_float_record(tb, r, temp_col->col_name,temp_col->value->f);
                        break;
                    case BOOL:
                        insert_boolean_record(tb, r, temp_col->col_name,temp_col->value->b);
                        break;
                    case STRING:
                        insert_string_record(tb, r, temp_col->col_name,temp_col->value->s);
                        break;
                }

                temp_col = temp_col->next;
            }
            write_record_to_page(r, curr_pg, off, get_columns_size(tb));
            write_page_to_file(f, curr_pg, curr_blk->id);

        }
        off += get_columns_size(tb);
    }

}

void delete_records_from_table(condition* cond, table* tb, database* db, FILE* f) {

    uint32_t pre_id = tb->first_record_block_id;

    page* curr_pg = allocate_page(db->block_size);
    read_page_from_file(f, curr_pg, tb->first_record_block_id);
    block* curr_blk = read_block_from_page(curr_pg);

    uint32_t next_id = curr_blk->next_id;

    page* last_pg = allocate_page(db->block_size);
    read_page_from_file(f, last_pg, tb->last_record_block_id);
    block* last_blk = read_block_from_page(last_pg);

    uint32_t deleted_rows = 0;
    uint32_t off = sizeof(block);

    bool block_flag = false;

    for(uint32_t i = 0; i < tb->num_rows; i++){

        record* r = read_record_from_page(tb, curr_pg, off);

        bool flag = true;
        condition* temp = cond;
        while(temp != NULL) {

            switch (temp->type) {
                case INTEGER:;
                    int32_t i_n = get_integer_record(tb, r, temp->col_name);
                    if(!check_integers(i_n, temp->value->i, temp->relation)){
                        flag = false;
                    }
                    break;
                case FLOAT:;
                    float f_n = get_float_record(tb, r, temp->col_name);
                    if(!check_floats(f_n, temp->value->f, temp->relation)){
                        flag = false;
                    }
                    break;
                case BOOL:;
                    bool b_n = get_boolean_record(tb, r, temp->col_name);
                    if(!check_bools(b_n, temp->value->b, temp->relation)){
                        flag = false;
                    }
                    break;
                case STRING:;
                    char* s = get_string_record(tb, r, temp->col_name);
                    if(!check_strings(s, temp->value->s, temp->relation)){
                        flag = false;
                    }
                    break;
            }

            if(!flag) {
                break;
            }
            temp = temp->next;
        }

        if(flag) {
            block_flag = true;
            r->valid = false;
            write_record_to_page(r, curr_pg, off, get_columns_size(tb));
            deleted_rows++;
        }
        off += get_columns_size(tb);

        if(off >= curr_blk->offset || i+1>=tb->num_rows){
            if(block_flag){
                if(pre_id != curr_blk->id) {
                    page* pre_pg = allocate_page(db->block_size);
                    read_page_from_file(f, pre_pg, pre_id);
                    block* pre_blk = read_block_from_page(pre_pg);

                    pre_blk->next_id = curr_blk->next_id;

                    write_block_to_page(pre_blk, pre_pg);
                    write_page_to_file(f, pre_pg, pre_id);

                    free(pre_blk);
                    free(pre_pg);
                } else {
                    tb->first_record_block_id = curr_blk->next_id;
                }
                next_id = curr_blk->next_id;
                write_to_last_record_block(f,
                                           curr_pg,
                                           curr_blk,
                                           last_pg,
                                           last_blk,
                                           tb,
                                           db);

                block_flag = false;
            }

            off = sizeof(block);
            pre_id = curr_blk->id;
            get_next_block(next_id, curr_blk,curr_pg, f);
        }

    }
    tb->num_rows -= deleted_rows;

}



