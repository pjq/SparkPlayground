package me.pjq;

public class Main {

    public static void main(String[] args) {
	// write your code here
        WordCount wordCount = new WordCount();
        String path = "/Users/pengjianqing/workspace/SparkPlayground/LICENSE";
        wordCount.count(path);
    }
}
