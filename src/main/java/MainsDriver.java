import org.apache.hadoop.util.ProgramDriver;

/*
 * @class name:MainsDriver
 * @author:Wu Gang
 * @create: 2019-05-07 22:23
 * @description:
 */
public class MainsDriver {
    public static void main(String[] argv){
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("wordcount", WordCount.class, "A map/reduce program that counts the words in the input files.");
            pgd.addClass("tfidf", TFIDF.class, "A map/reduce program that counts tfidf of the words in the input files.");
            pgd.addClass("document tnverted", DocumentInverted.class, "文档到排序");
            pgd.addClass("sort frequency", SortFrequency.class, "排序");
           exitCode = pgd.run(argv);
        } catch (Throwable var4) {
            var4.printStackTrace();
        }

        System.exit(exitCode);
    }
}
