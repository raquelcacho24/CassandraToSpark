package com.upm.etsit.raquel.tfg;

import java.util.Date;

import twitter4j.HashtagEntity;




public class Tweet implements java.io.Serializable{
	private static final long serialVersionUID = -2599376376240068235L;
	private Date date;
	private String name;
	private String text;
	//private HashtagEntity[] hashtagEntities;
	private int retweets;
	private String country;
	
	
	public Tweet(Date date, String name, String text,  int retweets, String country) {
		this.date = date;
		this.text = text;
		this.name = name;
		//this.hashtagEntities = hashtagEntities;
		this.retweets = retweets;
		this.country = country;


	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public long getRetweets() {
		return retweets;
	}

	public void setRetweets(int retweets) {
		this.retweets = retweets;
	}

	/*public HashtagEntity[] getHashtag() {
		return hashtagEntities;
	}

	public void setHashtag(HashtagEntity[] hashtagEntities) {
		this.hashtagEntities = hashtagEntities;
	}*/

	public Date getDate() {
		return date;
	}
	
	public String getText() {
		return text;
	}

}