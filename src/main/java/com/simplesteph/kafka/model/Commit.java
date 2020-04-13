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
	private String owner;
	private String repo;

	
	
	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getRepo() {
		return repo;
	}

	public void setRepo(String repo) {
		this.repo = repo;
	}

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
		JSONObject jsonObject = commitBody.getJSONObject(COMMIT_FIELD);
		commit.setCommitMessage(jsonObject.getString(COMMIT_MESSAGE_FIELD));

		jsonObject = jsonObject.getJSONObject(COMMITTER_FIELD);
		commit.setCommittedAt(Instant.parse(jsonObject.getString(COMMITTED_AT_FIELD)));
		commit.setCommitterEmail(jsonObject.getString(USER_EMAIL_FIELD));
		commit.setCommitterName(jsonObject.getString(COMMIT_NAME_FIELD));

		return commit;

	}

}
