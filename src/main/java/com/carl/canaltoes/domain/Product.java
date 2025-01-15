package com.carl.canaltoes.domain;


/**
 * @author: carl
 * @date: 2025/1/15
 */
//@Document(indexName = "product")
public class Product {
//    @Id
    private String id;
//    @Field(type = FieldType.Text)
    private String name;

//    @Field(type = FieldType.Text)
    private String desc;

    public Product(String id, String name, String desc) {
        this.id = id;
        this.name = name;
        this.desc = desc;
    }

    public Product() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                '}';
    }
}
