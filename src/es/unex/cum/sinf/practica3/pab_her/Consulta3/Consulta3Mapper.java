package es.unex.cum.sinf.practica3.pab_her.Consulta3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Consulta 3 mapper.
 */
public class Consulta3Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        if (!values[20].equals(context.getConfiguration().get("region"))) return;

        String clave = values[20] + ";";

        Matcher m = Pattern.compile("\\d+-\\d+").matcher(values[1].replace("\"", ""));
        if (!m.find())
            return;

        clave += m.group();


        Double presion_atm = null;
        Double temperatura = null;
        Double humedad = null;


        try {
            presion_atm = Double.parseDouble(format(values[4])) < -9900d ? null : Double.parseDouble(format(values[4]));
        } catch (NumberFormatException ignored) {
        }
        try {
            temperatura = Double.parseDouble(format(values[8])) < -9900d ? null : Double.parseDouble(format(values[8]));
        } catch (NumberFormatException ignored) {
        }
        try {
            humedad = Double.parseDouble(format(values[16])) < -9900d ? null : Double.parseDouble(format(values[16]));
        } catch (NumberFormatException ignored) {
        }

        if (temperatura != null || humedad != null || presion_atm != null) {
            context.write(new Text(clave), new Text(String.format("%f;%f;%f", presion_atm, temperatura, humedad)));
        }
    }

    private String format(String value) {
        return value.trim();
    }
}
