package com.simplesteph.kafka;

import com.mashape.unirest.http.JsonNode;
import com.simplesteph.kafka.model.Commit;
import com.simplesteph.kafka.model.Issue;
import com.simplesteph.kafka.model.PullRequest;
import com.simplesteph.kafka.model.User;
import com.simplesteph.kafka.utils.DateUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import static com.simplesteph.kafka.GitHubSchemas.*;


public class GitHubSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(GitHubSourceTask.class);
    public GitHubSourceConnectorConfig config;

    protected Instant nextQuerySince;
    protected Integer lastIssueNumber;
    protected Integer nextPageToVisit = 1;
    protected Instant lastUpdatedAt;

    GitHubAPIHttpClient gitHubHttpAPIClient;
    BuildRecord buildRecord;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        //Do things here that are required to start your task. This could be open a connection to a database, etc.
        config = new GitHubSourceConnectorConfig(map);
        initializeLastVariables();
        gitHubHttpAPIClient = new GitHubAPIHttpClient(config);
        buildRecord = new BuildRecord(config);
        
    }

    private void initializeLastVariables(){
        Map<String, Object> lastSourceOffset = null;
        lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());
        if( lastSourceOffset == null){
            // we haven't fetched anything yet, so we initialize to 7 days ago
            nextQuerySince = config.getSince();
            lastIssueNumber = -1;
        } else {
            Object updatedAt = lastSourceOffset.get(UPDATED_AT_FIELD);
            Object issueNumber = lastSourceOffset.get(NUMBER_FIELD);
            Object nextPage = lastSourceOffset.get(NEXT_PAGE_FIELD);
            if(updatedAt != null && (updatedAt instanceof String)){
                nextQuerySince = Instant.parse((String) updatedAt);
            }
            if(issueNumber != null && (issueNumber instanceof String)){
                lastIssueNumber = Integer.valueOf((String) issueNumber);
            }
            if (nextPage != null && (nextPage instanceof String)){
                nextPageToVisit = Integer.valueOf((String) nextPage);
            }
        }
    }



    @Override
    public List<SourceRecord> poll() throws InterruptedException {

    	gitHubHttpAPIClient.sleepIfNeed();

    	// fetch data
    	// we'll count how many results we get with i
    	int i = 0;
    	final ArrayList<SourceRecord> records = new ArrayList<>();
    	String requestUrl="";
    	switch(config.getNodeConfig()) {
    	case "ISSUE":
    		requestUrl= String.format(
    				"https://api.github.com/repos/%s/%s/issues?page=%s&per_page=%s&since=%s&state=all&direction=asc&sort=updated",
    				config.getOwnerConfig(),
    				config.getRepoConfig(),
    				nextPageToVisit,
    				config.getBatchSize(),
    				nextQuerySince.toString());
    		JsonNode jsonResponse = gitHubHttpAPIClient.getNextItems(nextPageToVisit, nextQuerySince, requestUrl);
    		for (Object obj : jsonResponse.getArray()) {
    			Issue issue = Issue.fromJson((JSONObject) obj);
    			SourceRecord sourceRecord = new SourceRecord(
    					sourcePartition(),
    					sourceOffset(issue.getUpdatedAt()),
    					config.getTopic(),
    					null, // partition will be inferred by the framework
    					SCHEMA_ISSUE,
    					buildRecord.buildRecordValue(issue)
    					);
    			lastUpdatedAt = issue.getUpdatedAt();
    			records.add(sourceRecord);
    			i += 1;
    		}
    		break;
    	case "USER":
    		requestUrl= String.format(
    				"https://api.github.com/users/%s",
    				config.getOwnerConfig());
    		jsonResponse  = gitHubHttpAPIClient.getNextItems(nextPageToVisit, nextQuerySince, requestUrl);
    		User user = User.fromSCMJson(jsonResponse.getObject());
    		SourceRecord sourceRecord = new SourceRecord(
    				sourcePartition(),
    				sourceOffset(user.getUpdatedAt()),
    				config.getTopic(),
    				null, // partition will be inferred by the framework
    				SCHEMA_USER,
    				buildRecord.buildRecordValue(user));
    		lastUpdatedAt = user.getUpdatedAt();
    		records.add(sourceRecord);
    		i=1;
    		break;
    	case "COMMIT":
    		requestUrl= String.format(
    				"https://api.github.com/repos/%s/%s/commits?since=%s",
    				config.getOwnerConfig(),
    				config.getRepoConfig(),
    				nextQuerySince.toString());
    		jsonResponse = gitHubHttpAPIClient.getNextItems(nextPageToVisit, nextQuerySince, requestUrl);
    		for (Object obj : jsonResponse.getArray()) {
    			Commit commit = Commit.fromJson((JSONObject) obj);
    			sourceRecord = new SourceRecord(
    					sourcePartition(),
    					sourceOffset(commit.getCommittedAt()),
    					config.getTopic(),
    					null, // partition will be inferred by the framework
    					SCHEMA_COMMIT,
    					buildRecord.buildRecordValue(commit));
    			lastUpdatedAt = commit.getCommittedAt();
    			records.add(sourceRecord);
    			i += 1;
    		}
    		break;	

    	}

    	if (i > 0) log.info(String.format("Fetched %s record(s)", i));
    	if (i == 100){
    		// we have reached a full batch, we need to get the next one
    		nextPageToVisit += 1;
    	}
    	else {
    		nextQuerySince = lastUpdatedAt.plusSeconds(1);
    		nextPageToVisit = 1;
    		gitHubHttpAPIClient.sleep();
    	}
    	return records;
    
    }

    @Override
    public void stop() {
        // Do whatever is required to stop your task.
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(OWNER_FIELD, config.getOwnerConfig());
        map.put(REPOSITORY_FIELD, config.getRepoConfig());
        return map;
    }

    private Map<String, String> sourceOffset(Instant updatedAt) {
        Map<String, String> map = new HashMap<>();
        map.put(UPDATED_AT_FIELD, DateUtils.MaxInstant(updatedAt, nextQuerySince).toString());
        map.put(NEXT_PAGE_FIELD, nextPageToVisit.toString());
        return map;
    }


}