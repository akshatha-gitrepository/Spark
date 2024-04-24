package datasetapi;

public class Application {
    public static void main(String[] args) {
//        InferCSVSchema parser = new InferCSVSchema();
//        parser.printSchema();
//
//        DefineCSVSchema parser2 = new DefineCSVSchema();
//		parser2.printDefinedSchema();

        JsonLineParser parser3 = new JsonLineParser();
        parser3.parseJsonLines();
    }
}
