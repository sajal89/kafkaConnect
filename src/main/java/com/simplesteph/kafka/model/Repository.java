package com.simplesteph.kafka.model;

import static com.simplesteph.kafka.GitHubSchemas.*;

import java.time.Instant;

import org.json.JSONObject;

public class Repository {
	private String repoName;
	private String repoOwner;
	private String repoDescription;
	private Instant createdAt;

	public String getRepoName() {
		return repoName;
	}

	public void setRepoName(String repoName) {
		this.repoName = repoName;
	}

	public String getRepoDescription() {
		return repoDescription;
	}

	public void setRepoDescription(String repoDescription) {
		this.repoDescription = repoDescription;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Instant createddAt) {
		this.createdAt = createddAt;
	}

	public String getRepoOwner() {
		return repoOwner;
	}

	public void setRepoOwner(String repoOwner) {
		this.repoOwner = repoOwner;
	}

	public static Repository formJson(JSONObject jsonObject) {
		Repository repo = new Repository();
		repo.setCreatedAt(Instant.parse(jsonObject.getString(CREATED_AT_FIELD)));
		if (!jsonObject.isNull(REPO_DESC_FIELD))
			repo.setRepoDescription(jsonObject.getString(REPO_DESC_FIELD));
		repo.setRepoName(jsonObject.getString(COMMIT_NAME_FIELD));
		return repo;

	}

}
