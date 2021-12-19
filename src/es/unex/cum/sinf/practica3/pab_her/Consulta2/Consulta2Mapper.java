package es.unex.cum.sinf.practica3.pab_her.Consulta2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The type Consulta 2 mapper.
 */
public class Consulta2Mapper extends Mapper<Object, Text, Text, Text> {

    private static final String SEPARATOR = ",";

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String[] values = value.toString().split(SEPARATOR);

        String estacion = values[22];
        Integer anio;
        try {
            anio = Integer.parseInt(values[1].replace("\"", "").split("-")[0]);
        } catch (NumberFormatException e) {
            return;
        }

        if (!context.getConfiguration().get("estacion").equals(estacion) || anio != Integer.parseInt(context.getConfiguration().get("anio"))) {
            return;
        }

        Double presion_atm = null;
        Double radiacion_solar = null;
        Double temperatura = null;
        Double humedad = null;
        Double velocidad_viento = null;


        try {
            presion_atm = Double.parseDouble(format(values[4])) < -9900d ? null : Double.parseDouble(format(values[4]));
        } catch (NumberFormatException ignored) {
        }
        try {
            radiacion_solar = Double.parseDouble(format(values[7])) < -9900d ? null : Double.parseDouble(format(values[7]));
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
        try {
            velocidad_viento = Double.parseDouble(format(values[19])) < -9900d ? null : Double.parseDouble(format(values[19]));
        } catch (NumberFormatException ignored) {
        }

        if (temperatura != null || humedad != null || presion_atm != null || radiacion_solar != null) {
            context.write(new Text(estacion + ";" + anio), new Text(String.format("%f;%f;%f;%f;%f", presion_atm, radiacion_solar, temperatura, humedad, velocidad_viento)));
        }
    }

    private String format(String value) {
        return value.trim();
    }
}
