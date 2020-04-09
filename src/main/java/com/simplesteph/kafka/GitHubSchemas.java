package com.simplesteph.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

public class GitHubSchemas {

    public static final String NEXT_PAGE_FIELD = "next_page";

    // Issue fields
    public static final String OWNER_FIELD = "owner";
    public static final String REPOSITORY_FIELD = "repository";
    public static final String CREATED_AT_FIELD = "created_at";
    public static final String UPDATED_AT_FIELD = "updated_at";
    public static final String NUMBER_FIELD = "number";
    public static final String URL_FIELD = "url";
    public static final String HTML_URL_FIELD = "html_url";
    public static final String TITLE_FIELD = "title";
    public static final String STATE_FIELD = "state";

    public static final String BODY_FIELD = "body";
    public static final String ID_FIELD = "id";
    public static final String TYPE_FIELD = "node_type";

    // SCM User fields
    public static final String USER_FIELD = "user";
    public static final String USER_URL_FIELD = "url";
    public static final String USER_HTML_URL_FIELD = "html_url";
    public static final String USER_ID_FIELD = "id";
    public static final String USER_LOGIN_FIELD = "login";
    public static final String USER_EMAIL_FIELD = "email";
    public static final String USER_COMPANY_FIELD = "company";
    public static final String USER_LOCATION_FIELD = "location";
    
    //Commit fields
    public static final String COMMIT_FIELD = "commit";
    public static final String COMMITTER_FIELD = "committer";
    public static final String COMMIT_NAME_FIELD = "name";
    public static final String COMMIT_MESSAGE_FIELD = "message";
    public static final String COMMITTED_AT_FIELD = "date";

    // PR fields
    public static final String PR_FIELD = "pull_request";
    public static final String PR_URL_FIELD = "url";
    public static final String PR_HTML_URL_FIELD = "html_url";

    // Schema names
    public static final String SCHEMA_KEY = "com.simplesteph.kafka.connect.github.IssueKey";
    public static final String SCHEMA_VALUE_ISSUE = "com.simplesteph.kafka.connect.github.IssueValue";
    public static final String SCHEMA_VALUE_USER = "com.simplesteph.kafka.connect.github.UserValue";
    public static final String SCHEMA_VALUE_COMMIT = "com.simplesteph.kafka.connect.github.CommitValue";
    public static final String SCHEMA_VALUE_PR = "com.simplesteph.kafka.connect.github.PrValue";

    
    //Issue Schema for pulse
    public static final Schema SCHEMA_ISSUE = SchemaBuilder.struct().name(SCHEMA_VALUE_ISSUE)
            .version(1)
            .field(TYPE_FIELD, Schema.STRING_SCHEMA)
            .field(ID_FIELD, Schema.INT32_SCHEMA)
            .field(TITLE_FIELD, Schema.STRING_SCHEMA)
            .field(STATE_FIELD, Schema.STRING_SCHEMA)
            .field(BODY_FIELD, Schema.STRING_SCHEMA)
            .field(USER_FIELD, Schema.STRING_SCHEMA) // mandatory
            .field(OWNER_FIELD, Schema.STRING_SCHEMA)
            .field(REPOSITORY_FIELD, Schema.STRING_SCHEMA)
            .field(CREATED_AT_FIELD, Timestamp.SCHEMA)
            .field(UPDATED_AT_FIELD, Timestamp.SCHEMA)
            .field(NUMBER_FIELD, Schema.INT32_SCHEMA)
            .build();
    
    // Value Schema
    public static final Schema SCHEMA_USER = SchemaBuilder.struct().name(SCHEMA_VALUE_USER)
            .version(1)
            .field(TYPE_FIELD, Schema.STRING_SCHEMA)
            .field(USER_URL_FIELD, Schema.STRING_SCHEMA)
            .field(USER_ID_FIELD, Schema.INT32_SCHEMA)
            .field(USER_LOGIN_FIELD, Schema.STRING_SCHEMA)
            .field(USER_EMAIL_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(USER_COMPANY_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(USER_LOCATION_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CREATED_AT_FIELD, Timestamp.SCHEMA)
            .field(UPDATED_AT_FIELD, Timestamp.SCHEMA)
            .build();
    
 // Commit Schema
    public static final Schema SCHEMA_COMMIT = SchemaBuilder.struct().name(SCHEMA_VALUE_COMMIT)
            .version(2)
            .field(TYPE_FIELD, Schema.STRING_SCHEMA)
//            .field(USER_LOGIN_FIELD, Schema.STRING_SCHEMA)
            .field(USER_EMAIL_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(COMMIT_NAME_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(COMMIT_MESSAGE_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(COMMITTED_AT_FIELD, Timestamp.SCHEMA)
            .build();
}
