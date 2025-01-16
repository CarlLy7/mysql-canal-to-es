package com.carl.canaltoes.domain;

/**
 * @author: carl
 * @date: 2025/1/16
 */

public class User{
    private String id;
    private String username;

    private String like;

    public User(String id, String username, String like) {
        this.id = id;
        this.username = username;
        this.like = like;
    }

    public User() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getLike() {
        return like;
    }

    public void setLike(String like) {
        this.like = like;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", username='" + username + '\'' +
                ", like='" + like + '\'' +
                '}';
    }
}
