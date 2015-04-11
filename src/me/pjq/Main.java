package me.pjq;

public class Main {

    public static void main(String[] args) {
//        WordCount wordCount = new WordCount();
//        wordCount.count("LICENSE", "LICENSE WordCount");

        CSDNWordAnalyse csdnWordAnalyse = new CSDNWordAnalyse();
        csdnWordAnalyse.count("/Volumes/NTFS/cloudary/E/www.csdn.net.sql", "csdn 600万用户名密码");
//        csdnWordAnalyse.count("data/www.csdn.net.sql", "csdn 600万用户名密码");
    }
}
