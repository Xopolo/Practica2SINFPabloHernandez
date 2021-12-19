package es.unex.cum.sinf.practica3.pab_her.Consulta7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Consulta 7 mapper.
 */
public class Consulta7Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);


        Matcher m = Pattern.compile("\\d+").matcher(values[1].replace("\"", ""));
        String[] cads = new String[3];
        int index = 0;
        while (m.find()) {
            cads[index] = m.group();
            index++;
        }


        if (values[20].equals(context.getConfiguration().get("region")) &&
                cads[0].equals(context.getConfiguration().get("anio")) &&
                cads[1].equals(context.getConfiguration().get("mes"))) {


            String clave = values[20] + ";" + cads[0] + "-" + cads[1] + ";" + values[2].replace("\"", "");

            Double precipitacion = null;

            try {
                precipitacion = Double.parseDouble(format(values[3])) < -9900d ? null : Double.parseDouble(format(values[3]));
            } catch (NumberFormatException ignored) {
            }


            if (precipitacion != null) {
                context.write(new Text(clave), new Text(String.format("%f", precipitacion)));
            }
        }

    }

    private String format(String value) {
        return value.trim();
    }
}
