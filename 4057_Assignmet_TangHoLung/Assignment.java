import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Assignment {

    public static class bibleMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text one = new Text();
        private Text bKey = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //valuesable library
            String lines = value.toString();     //take all lines in bible
            ArrayList<Line> arrays = new ArrayList<Line>(); //arraylist to save each book one by one

            //split the bible line by line
            StringTokenizer tokenizer = new StringTokenizer(lines, "\n");

            //if having next line, do the split to split the book,chapter,verse,content
            try {
                while (tokenizer.hasMoreTokens()) {
                    Line line = new Line();
                    String bookName = "";

                    //word = current line
                    bKey.set(tokenizer.nextToken());

                    context.write(bKey, one);

                    //e.g "1 Chronicles 8:7 And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud."
                    //1 Chronicles 8:7[0] And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud.[1]
                    bKey.toString().split("\t");

                    //And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud.
                    line.setContent(bKey.toString().split("\t")[1]);

                    //1 Chronicles 8:7  *size-1 = book name
                    String[] arr = bKey.toString().split("\t")[0].split(" ");

                    //1[0] Chronicles[1] 8:7[2]
                    for (int i = 0; i < arr.length-1 ; i++){
                        //1 Chronicles
                        if (i == 0){
                            bookName += arr[i];
                        }else{
                            bookName += " "+arr[i];
                        }

                    }
                    line.setBook(bookName);

                    //8
                    line.setChapter(arr[arr.length-1].split(":")[0]);
                    bKey.set(line.getChapter());

                    //7
                    line.setVerse(arr[arr.length-1].split(":")[1]);

                    //add the object to the arraylist
                    arrays.add(line);
                }
            }catch (Exception e) {
                System.out.println("Error");
                e.printStackTrace();
            }
        }
    }

    public static class bibleReducer
            extends Reducer<Text, Text, Text, Text> {
        Text one = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            context.write(key,one);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Bible <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "AssignmentQ1 by TangHoLung 21220670");
        job.setJarByClass(Assignment.class);
        job.setMapperClass(bibleMapper.class);
        job.setReducerClass(bibleReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Line{
        private String book;
        private String chapter;
        private String verse;
        private String content;
        private String line;

        public Line(String book, String chapter, String verse, String content) {
            this.book = book;
            this.chapter = chapter;
            this.verse = verse;
            this.content = content;
        }

        public Line() {}

        public String getBook() {return book;}
        public String getChapter() {return chapter;}
        public String getVerse() {return verse;}
        public String getContent() {return content;}

        public void setBook(String book) {this.book = book;}
        public void setChapter(String chapter) {this.chapter = chapter;}
        public void setVerse(String verse) {this.verse = verse;}
        public void setContent(String content) {this.content = content;}
    }
}
