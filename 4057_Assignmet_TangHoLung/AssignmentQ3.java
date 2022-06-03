import java.io.IOException;
import java.util.*;

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

public class AssignmentQ3 {

    public static class bibleMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text bKey = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //valuesable library
            String lines = value.toString().toLowerCase();//take all lines in bible

            ArrayList<Line> arrays = new ArrayList<Line>(); //arraylist to save each book one by one

            //split the bible line by line
            StringTokenizer tokenizer = new StringTokenizer(lines, "\n");

            //if having next line, do the split to split the content
            try {
                while (tokenizer.hasMoreTokens()) {
                    String content = "";

                    //bKey = current line
                    bKey.set(tokenizer.nextToken());

                    //e.g "1 Chronicles 8:7 And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud."

                    //And Naaman, and Ahiah, and Gera, he removed them, and begat Uzza, and Ahihud.
                    content = bKey.toString().split("\t")[1];

                    //"And" "Naaman," "and" "Ahiah," "and" "Gera," "he" "removed" "them," "and" "begat" "Uzza," "and" "Ahihud."
                    String[] contentWords = content.split(" ");

                    //loop the arr that is no stop word and do content.write
                    for (int i = 0 ; i < contentWords.length ; i++){
                        String word = contentWords[i].replaceAll("[^a-zA-Z0-9]","");    //replaceAll special character to ""
                        if (!Stopwords.isOneOfThem(word)){
                            Text t = new Text(word);
                            context.write(t, one );
                        }
                    }
                }
            }catch (Exception e) {
                System.out.println("Error");
                e.printStackTrace();
            }

        }
    }

    public static class bibleReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        //sort the item first, only same things shown
        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {

            //wordcount
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            //only show the result more that 3000 times
            if (sum >= 3000){
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Bible <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "AssignmentQ3 by TangHoLung 21220670");
        job.setJarByClass(AssignmentQ2.class);
        job.setMapperClass(bibleMapper.class);
        job.setReducerClass(bibleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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

    static class Stopwords {
        public static String[] myStopWordsArray =
                {"a",
                        "about",
                        "above",
                        "across",
                        "after",
                        "afterwards",
                        "again",
                        "against",
                        "all",
                        "almost",
                        "alone",
                        "along",
                        "already",
                        "also",
                        "although",
                        "always",
                        "am",
                        "among",
                        "amongst",
                        "amoungst",
                        "amount",
                        "an",
                        "and",
                        "another",
                        "any",
                        "anyhow",
                        "anyone",
                        "anything",
                        "anyway",
                        "anywhere",
                        "are",
                        "around",
                        "as",
                        "at",
                        "back",
                        "be",
                        "became",
                        "because",
                        "become",
                        "becomes",
                        "becoming",
                        "been",
                        "before",
                        "beforehand",
                        "behind",
                        "being",
                        "below",
                        "beside",
                        "besides",
                        "between",
                        "beyond",
                        "bill",
                        "both",
                        "bottom",
                        "but",
                        "by",
                        "by",
                        "call",
                        "can",
                        "common",
                        "cannot",
                        "cant",
                        "co",
                        "computer",
                        "con",
                        "could",
                        "couldnt",
                        "cry",
                        "de",
                        "describe",
                        "detail",
                        "do",
                        "does",
                        "done",
                        "down",
                        "due",
                        "during",
                        "each",
                        "eg",
                        "eight",
                        "either",
                        "eleven",
                        "else",
                        "elsewhere",
                        "empty",
                        "enough",
                        "etc",
                        "even",
                        "ever",
                        "every",
                        "everyone",
                        "everything",
                        "everywhere",
                        "except",
                        "few",
                        "fifteen",
                        "fify",
                        "fill",
                        "find",
                        "fire",
                        "first",
                        "five",
                        "for",
                        "former",
                        "formerly",
                        "forty",
                        "found",
                        "four",
                        "from",
                        "front",
                        "full",
                        "further",
                        "get",
                        "give",
                        "go",
                        "had",
                        "has",
                        "hasnt",
                        "have",
                        "he",
                        "hence",
                        "her",
                        "here",
                        "hereafter",
                        "hereby",
                        "herein",
                        "hereupon",
                        "hers",
                        "herself",
                        "him",
                        "himself",
                        "his",
                        "how",
                        "however",
                        "hundred",
                        "i",
                        "ie",
                        "if",
                        "in",
                        "inc",
                        "indeed",
                        "interest",
                        "into",
                        "is",
                        "it",
                        "its",
                        "itself",
                        "keep",
                        "last",
                        "latter",
                        "latterly",
                        "least",
                        "less",
                        "ltd",
                        "made",
                        "many",
                        "may",
                        "me",
                        "meanwhile",
                        "might",
                        "mill",
                        "mine",
                        "more",
                        "moreover",
                        "most",
                        "mostly",
                        "move",
                        "much",
                        "must",
                        "my",
                        "myself",
                        "name",
                        "namely",
                        "neither",
                        "never",
                        "nevertheless",
                        "next",
                        "nine",
                        "no",
                        "nobody",
                        "none",
                        "noone",
                        "nor",
                        "not",
                        "nothing",
                        "now",
                        "nowhere",
                        "of",
                        "off",
                        "often",
                        "on",
                        "once",
                        "one",
                        "only",
                        "onto",
                        "or",
                        "other",
                        "others",
                        "otherwise",
                        "our",
                        "ours",
                        "ourselves",
                        "out",
                        "over",
                        "own",
                        "part",
                        "per",
                        "perhaps",
                        "please",
                        "put",
                        "rather",
                        "re",
                        "same",
                        "see",
                        "seem",
                        "seemed",
                        "seeming",
                        "seems",
                        "serious",
                        "several",
                        "she",
                        "should",
                        "show",
                        "side",
                        "since",
                        "sincere",
                        "six",
                        "sixty",
                        "so",
                        "some",
                        "somehow",
                        "someone",
                        "something",
                        "sometime",
                        "sometimes",
                        "somewhere",
                        "still",
                        "such",
                        "system",
                        "take",
                        "ten",
                        "than",
                        "that",
                        "the",
                        "their",
                        "them",
                        "themselves",
                        "then",
                        "thence",
                        "there",
                        "thereafter",
                        "thereby",
                        "therefore",
                        "therein",
                        "thereupon",
                        "these",
                        "they",
                        "thick",
                        "thin",
                        "third",
                        "this",
                        "those",
                        "though",
                        "three",
                        "through",
                        "throughout",
                        "thru",
                        "thus",
                        "to",
                        "together",
                        "too",
                        "top",
                        "toward",
                        "towards",
                        "twelve",
                        "twenty",
                        "two",
                        "un",
                        "under",
                        "until",
                        "up",
                        "upon",
                        "us",
                        "usually",
                        "usual",
                        "very",
                        "via",
                        "was",
                        "we",
                        "well",
                        "were",
                        "what",
                        "whatever",
                        "when",
                        "whence",
                        "whenever",
                        "where",
                        "whereafter",
                        "whereas",
                        "whereby",
                        "wherein",
                        "whereupon",
                        "wherever",
                        "whether",
                        "which",
                        "while",
                        "whither",
                        "who",
                        "whoever",
                        "whole",
                        "whom",
                        "whose",
                        "why",
                        "will",
                        "with",
                        "within",
                        "without",
                        "would",
                        "yet",
                        "you",
                        "your",
                        "yours",
                        "yourself",
                        "yourselves",
                        "don't",
                        "won't",
                        "can't",
                        "didn't",
                        "it's",
                        "is'nt",
                        "isn't",
                        "aren't",
                        "wasn't",
                        "haven't",
                        "hasn't",
                        "hadn't",
                        "you've",
                        "it'hv",
                        "you'd",
                        "you're",
                        "hasn't",
                        "we'll",
                        "you're",
                        "we're",
                        "we've"};

        public static Set<String> myStopWords = new HashSet<String>(Arrays.asList(myStopWordsArray));

        public static boolean isOneOfThem(String in) {
            return myStopWords.contains(in);
        }

    }
}