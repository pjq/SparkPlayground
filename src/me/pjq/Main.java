package me.pjq;

public class Main {

    public static void main(String[] args) {
//        WordCount wordCount = new WordCount();
//        wordCount.startAnalyse("LICENSE", "LICENSE WordCount");

        CSDNWordAnalyse csdnWordAnalyse = new CSDNWordAnalyse();
//        csdnWordAnalyse.startAnalyse("/Volumes/NTFS/cloudary/E/www.csdn.net.sql", "csdn 6 million password ");
        csdnWordAnalyse.startAnalyse("data/www.csdn.net.sql", "csdn 10000 password ");
    }
}
