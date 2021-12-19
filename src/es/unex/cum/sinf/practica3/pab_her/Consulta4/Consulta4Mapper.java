package es.unex.cum.sinf.practica3.pab_her.Consulta4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Consulta 4 mapper.
 */
public class Consulta4Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        String clave;


        Pattern compile = Pattern.compile("^\\d+-\\d+");
        Matcher m = compile.matcher(values[1].replace("\"", ""));
        if (!m.find())
            return;

        clave = m.group();
        // Se supone que es el anio
        if (!clave.split("-")[0].equals(context.getConfiguration().get("anio"))) return;


        Double velocidad_viento = null;

        try {
            velocidad_viento = Double.parseDouble(format(values[19])) < -9900d ? null : Double.parseDouble(format(values[19]));
        } catch (NumberFormatException ignored) {
        }

        if (velocidad_viento != null) {
            context.write(new Text(clave), new Text(String.format("%f", velocidad_viento)));
        }

    }

    private String format(String value) {
        return value.trim();
    }
}
