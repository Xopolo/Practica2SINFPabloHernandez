package es.unex.cum.sinf.practica3.pab_her.Consulta6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The type Consulta 6 mapper.
 */
public class Consulta6Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        String fecha = values[1].replace("\"", "");
        String region = values[20];
        String estacion = values[22];
        Double precipitacion = null;

        try {
            precipitacion = Double.parseDouble(format(values[3])) < -9900d ? null : Double.parseDouble(format(values[3]));
        } catch (NumberFormatException ignored) {
        }


        if (precipitacion != null) {
            context.write(new Text(""), new Text(String.format("%s;%s;%s;%f", fecha, region, estacion, precipitacion)));
        }

    }

    private String format(String value) {
        return value.trim();
    }
}
