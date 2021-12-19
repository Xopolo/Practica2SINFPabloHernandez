package es.unex.cum.sinf.practica3.pab_her.Consulta5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Consulta 5 mapper.
 */
public class Consulta5Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        String clave = values[22] + ";";

        Matcher m = Pattern.compile("\\d+").matcher(values[1].replace("\"", ""));
        if (!m.find())
            return;
        clave += m.group();

        Double temperatura = null;
        Double humedad = null;
        Double presion_atm = null;
        Double radiacion_solar = null;

        try {
            presion_atm = Double.parseDouble(format(values[4])) < -9900d ? null : Double.parseDouble(format(values[4]));
        } catch (NumberFormatException ignored) {}
        try {
            radiacion_solar = Double.parseDouble(format(values[7])) < -9900d ? null : Double.parseDouble(format(values[7]));
        } catch (NumberFormatException ignored) {}
        try {
            temperatura = Double.parseDouble(format(values[8])) < -9900d ? null : Double.parseDouble(format(values[8]));
        } catch (NumberFormatException ignored) {}
        try {
            humedad = Double.parseDouble(format(values[16])) < -9900d ? null : Double.parseDouble(format(values[16]));
        } catch (NumberFormatException ignored) {}


        if (temperatura != null || humedad != null || presion_atm != null || radiacion_solar != null) {
            context.write(new Text(clave), new Text(String.format("%f;%f;%f;%f", presion_atm, radiacion_solar, temperatura, humedad)));
        }

    }

    private String format(String value) {
        return value.trim();
    }
}
