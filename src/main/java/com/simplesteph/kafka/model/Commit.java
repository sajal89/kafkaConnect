package com.simplesteph.kafka.model;

import static com.simplesteph.kafka.GitHubSchemas.*;

import java.time.Instant;

import org.json.JSONObject;

public class Commit {
	private String committerLogin;
	private String committerEmail;
	private String committerName;
	private String commitMessage;
	private Instant committedAt;
	
	
	
	public String getCommitterLogin() {
		return committerLogin;
	}
	public void setCommitterLogin(String committerLogin) {
		this.committerLogin = committerLogin;
	}
	public String getCommitterEmail() {
		return committerEmail;
	}
	public void setCommitterEmail(String committerEmail) {
		this.committerEmail = committerEmail;
	}
	public String getCommitterName() {
		return committerName;
	}
	public void setCommitterName(String committerName) {
		this.committerName = committerName;
	}
	public String getCommitMessage() {
		return commitMessage;
	}
	public void setCommitMessage(String commitMessage) {
		this.commitMessage = commitMessage;
	}
	public Instant getCommittedAt() {
		return committedAt;
	}
	public void setCommittedAt(Instant committedAt) {
		this.committedAt = committedAt;
	}
	
	public static Commit fromJson(JSONObject commitBody) {
		Commit commit = new Commit();
		if(commitBody.has(COMMIT_FIELD)) {
			JSONObject jsonObject =commitBody.getJSONObject(COMMIT_FIELD);
			if(jsonObject.has(COMMITTER_FIELD)) {
				jsonObject =jsonObject.getJSONObject(COMMITTER_FIELD);
				
				commit.setCommittedAt(Instant.parse(jsonObject.getString(COMMITTED_AT_FIELD)));
				if(!jsonObject.isNull(USER_EMAIL_FIELD))
					commit.setCommitterEmail(jsonObject.getString(USER_EMAIL_FIELD));
		    	if(!jsonObject.isNull(COMMIT_NAME_FIELD))
		    		commit.setCommitterName(jsonObject.getString(COMMIT_NAME_FIELD));
		    	if(!jsonObject.isNull(COMMIT_MESSAGE_FIELD))
		    		commit.setCommitMessage(jsonObject.getString(COMMIT_MESSAGE_FIELD));
			}
		}
//		if(commitBody.has(COMMITTER_FIELD)) {
//			JSONObject jsonObject =commitBody.getJSONObject(COMMITTER_FIELD);
//			commit.setCommitterLogin(jsonObject.getString(USER_LOGIN_FIELD));
//		}
		return commit;

	}

}
