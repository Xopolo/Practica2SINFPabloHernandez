package es.unex.cum.sinf.practica3.pab_her;

import es.unex.cum.sinf.practica3.pab_her.Consulta1.Consulta1Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta1.Consulta1Reducer;
import es.unex.cum.sinf.practica3.pab_her.Consulta2.Consulta2Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta2.Consulta2Reducer;
import es.unex.cum.sinf.practica3.pab_her.Consulta3.Consulta3Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta3.Consulta3Reducer;
import es.unex.cum.sinf.practica3.pab_her.Consulta4.Consulta4Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta4.Consulta4Reducer;
import es.unex.cum.sinf.practica3.pab_her.Consulta5.Consulta5Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta5.Consulta5Reducer;
import es.unex.cum.sinf.practica3.pab_her.Consulta6.Consulta6Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta6.Consulta6Reducer;
import es.unex.cum.sinf.practica3.pab_her.Consulta7.Consulta7Mapper;
import es.unex.cum.sinf.practica3.pab_her.Consulta7.Consulta7Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * The type Consulta manager.
 */
public class ConsultaManager extends Configured implements Tool {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ConsultaManager(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        final Job job = new Job(getConf());
        job.getConfiguration().set("mapreduce.job.priority", JobPriority.VERY_HIGH.toString());
        if (args.length >= 3) {
            switch (Integer.parseInt(args[2])) {
                case 1:
                    job.setMapperClass(Consulta1Mapper.class);
                    job.setReducerClass(Consulta1Reducer.class);
                    break;

                case 2:
                    if (args.length == 5) {
                        job.setMapperClass(Consulta2Mapper.class);
                        job.setReducerClass(Consulta2Reducer.class);
                        job.getConfiguration().set("estacion", args[3]);
                        job.getConfiguration().set("anio", args[4]);
                    } else {
                        System.err.println("Consulta 2 requiere los parametros: {input file} {output dir} 2 {Nombre_estacion} {Year} ");
                        System.exit(-1);
                    }
                    break;

                case 3:
                    if (args.length == 4) {
                        job.setMapperClass(Consulta3Mapper.class);
                        job.setReducerClass(Consulta3Reducer.class);
                        job.getConfiguration().set("region", args[3]);
                    } else {
                        System.err.println("Consulta 3 requiere los parametros: {input file} {output dir} 3 {Region}");
                        System.exit(-1);
                    }
                    break;

                case 4:
                    if (args.length == 4) {
                        job.setMapperClass(Consulta4Mapper.class);
                        job.setReducerClass(Consulta4Reducer.class);
                        job.getConfiguration().set("anio", args[3]);
                    } else {
                        System.err.println("Consulta 4 requiere los parametros: {input file} {output dir} 4 {Anio}");
                        System.exit(-1);
                    }
                    break;

                case 5:
                    job.setMapperClass(Consulta5Mapper.class);
                    job.setReducerClass(Consulta5Reducer.class);
                    break;

                case 6:
                    job.setMapperClass(Consulta6Mapper.class);
                    job.setReducerClass(Consulta6Reducer.class);
                    break;

                case 7:
                    if (args.length == 6) {
                        job.setMapperClass(Consulta7Mapper.class);
                        job.setReducerClass(Consulta7Reducer.class);
                        job.getConfiguration().set("region", args[3]);
                        job.getConfiguration().set("anio", args[4]);
                        job.getConfiguration().set("mes", args[5]);

                    } else {
                        System.err.println("Consulta 7 requiere los parametros: {input file} {output dir} 7 {Region} {Anio} {Mes}");
                        System.exit(-1);
                    }
                    break;

                default:
                    System.err.println("Consulta requiere los parametros: {input file} {output dir} {[1-7]}");
                    System.exit(-1);
                    break;
            }

        } else {
            System.err.println("Consulta requiere los parametros: {input file} {output dir} {numero consulta}");
            System.exit(-1);

        }

        deleteOutputFileIfExists(args);


        job.setJarByClass(ConsultaManager.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setMapperClass(Consulta1Mapper.class);
//        job.setReducerClass(Consulta1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    private void deleteOutputFileIfExists(String[] args) throws IOException {
        final Path output = new Path(args[1]);
        FileSystem.get(output.toUri(), getConf()).delete(output, true);
    }
}
