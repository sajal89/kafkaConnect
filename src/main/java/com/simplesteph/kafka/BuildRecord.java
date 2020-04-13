package com.simplesteph.kafka;

import static com.simplesteph.kafka.GitHubSchemas.*;

import java.util.Date;

import org.apache.kafka.connect.data.Struct;

import com.simplesteph.kafka.model.Commit;
import com.simplesteph.kafka.model.Issue;
import com.simplesteph.kafka.model.User;

public class BuildRecord {
	
	 GitHubSourceConnectorConfig config;

	 
	public BuildRecord(GitHubSourceConnectorConfig config) {
		this.config = config;
	}
	
	 //Build Issue Record Value
    public Struct buildRecordValue(Issue issue){
        // Issue top level fields
        Struct valueStruct = new Struct(SCHEMA_ISSUE)
                .put(TYPE_FIELD, "ISSUE")
                .put(ID_FIELD, issue.getId())
                .put(TITLE_FIELD, issue.getTitle())
                .put(STATE_FIELD, issue.getState())
                .put(BODY_FIELD, issue.getBody())
                .put(USER_FIELD, issue.getUser().getLogin()) // mandatory
                .put(OWNER_FIELD, issue.getOwner())
                .put(REPOSITORY_FIELD, issue.getRepo())
                .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
                .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
                .put(NUMBER_FIELD, issue.getNumber());
//        System.out.println("data: " +valueStruct.toString());
        return valueStruct;
    }
    
    //Build User Record Value
    public Struct buildRecordValue(User user){
        // User top level fields
        Struct valueStruct = new Struct(SCHEMA_USER)
                .put(TYPE_FIELD, "User")
                .put(USER_URL_FIELD, user.getUrl())
                .put(USER_ID_FIELD, user.getId())
                .put(USER_LOGIN_FIELD, user.getLogin())// mandatory
                .put(CREATED_AT_FIELD, Date.from(user.getCreatedAt()))
                .put(UPDATED_AT_FIELD, Date.from(user.getUpdatedAt()));
        
        if(user.getEmail() != null)
        	valueStruct.put(USER_EMAIL_FIELD, user.getEmail());
        if(user.getCompany() != null)
        	valueStruct.put(USER_COMPANY_FIELD, user.getCompany());
        if(user.getLocation() != null)
        	valueStruct.put(USER_LOCATION_FIELD, user.getLocation()); 
        
//        System.out.println("data: " +valueStruct.toString());
        return valueStruct;
    }
    
    //build committer record value
    public Object buildRecordValue(Commit commit) {
    	Struct valueStruct = new Struct(SCHEMA_COMMIT)
    			.put(TYPE_FIELD, "COMMIT")
//    			.put(USER_LOGIN_FIELD, commit.getCommitterLogin())
    			.put(COMMITTED_AT_FIELD, Date.from(commit.getCommittedAt()))
    			.put(OWNER_FIELD, commit.getOwner())
    	        .put(REPOSITORY_FIELD, commit.getRepo());
    	
    	if(commit.getCommitterEmail() != null)
    		valueStruct.put(USER_EMAIL_FIELD, commit.getCommitterEmail());
    	if(commit.getCommitterName()!= null)
    		valueStruct.put(COMMIT_NAME_FIELD, commit.getCommitterName());
    	if(commit.getCommitMessage() != null)
    		valueStruct.put(COMMIT_MESSAGE_FIELD, commit.getCommitMessage());

//    	System.out.println("data: " +valueStruct.toString());
    	return valueStruct;
    }

	 
	 

}
