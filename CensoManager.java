import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.text.DecimalFormat;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import jdk.nashorn.internal.runtime.Context;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import static java.nio.charset.StandardCharsets.*;


/*
 * Bibliografía utilizada:
 * 	https://hadoop.apache.org/docs/current/api/
 * 	http://kickstarthadoop.blogspot.com.es/2011/09/joins-with-plain-map-reduce.html
 *  https://vangjee.wordpress.com/2012/03/07/the-in-mapper-combining-design-pattern-for-mapreduce-programming/
 */

public class CensoManager extends Configured implements Tool {

	/* Mapper que se utiliza para crear clave y valor del fichero de censoComercios.csv
	 * el fichero contiene comercios de La Laguna. Utilizo como clave el nombre de la
	 * calle y cuento el número de comercios que hay en una calle. Para ello utilizo una 
	 * técnica llamada In-Map-Combiner, esto quiere decir, que hago una combinación en el
	 * propio MAPPER y no utilizando un COMBINER, ya que, para mi caso, no me es válido.
	 * Para tal fin, utilizo un hashmap para guardar los datos que lee el MAPPER y para
	 * cada clave, cuento el número de comercios. Despues de ello, solo quedaría recorrer
	 * el hashmap e introducir cada par clave-valor en el Context. Al ejecutarse en paralelo
	 * para que todo concuerde, se debe sobrecargar el método setup y cleanup de la clase
	 * MAPPER.
	 */ 
	public static class ComerciosMapper extends Mapper<Object, Text, Text, Text> {
		
        private static final String SEPARATOR2 = ",";
        private String tag = "CO~"; //Utilizo este tag para diferenciar los pares clave-valor de número de comercios
        private Map<String, Integer> comerciosCounter; //Hashmap que será visible para cualquier instancia del método map 
        private final Text wordText = new Text(); //Variable local de texto
        private final Text valueText = new Text(); //Variable local de texto
        
   	 	/* setup -> Globaliza para cualquier instancia del método map que se ejecute en 
   	 	 * paralelo la variable hashmap. Yo lo veo como una memoria compartida entre los
   	 	 * diferentes instancias del método map.
   	 	 */
        @Override  
        protected void setup(Context context) throws IOException, InterruptedException {  
            super.setup(context);  
            comerciosCounter = new HashMap<String, Integer>();  
        }  
        
        
        /* Método donde donde se van incluyendo los pares clave-valor al hashmap
         * 
         */
		public void map(Object key, Text censo, Context context) throws IOException, InterruptedException {
		
			final String[] comercios = censo.toString().split(SEPARATOR2);
			//final String nombreComercio = format(comercios[1]); //Lo cojo pero no lo uso para nada
			final String nombreCalle = format(comercios[5]);
			
			/*Si no existe la clave se introduce un nuevo par clave-valor, si ya existe se añade uno*/
			if (comerciosCounter.containsKey(nombreCalle)) {
                comerciosCounter.put(nombreCalle, comerciosCounter.get(nombreCalle) + 1);  
            } else {  
                comerciosCounter.put(nombreCalle, 1);  
            }  
		}
		
		
		/* cleanup -> Una vez que se terminen de ejecutar las instancias del map, se ejecuta este
		 * método, que es el que finalmente va a trasbasar las información del hashmap al context
		 */ 
	    @Override  
	    protected void cleanup(Context context) throws IOException, InterruptedException {  
	    	
	        for (Map.Entry<String, Integer> entry : comerciosCounter.entrySet()) {  
	            wordText.set(entry.getKey());  
	            valueText.set(Integer.toString(entry.getValue()));
	            context.write(wordText, new Text(tag+valueText));  
	        }  
	        
	        super.cleanup(context);  
	    }  
	    
	    
	    
		/* Función para limpiar espacios
		 * 
		 */
		private String format(String cadena) {
			return cadena.trim();
		}
	}
	
	/* Mapper que se utliza para recolectar los datos del callejero, del cual, se obtiene los
	 * pares clave-valor (nombre de calle y codigo de calle).
	 * No se realiza ningún tratamiento especial a los datos.
	 */
	public static class CallejeroMapper extends Mapper<Object, Text, Text, Text> {

        private static final String SEPARATOR2 = ",";
        private String tag = "CA~"; //Utilizo este tag para diferenciar los pares clave-valor de codigo de calles

		public void map(Object key, Text censo, Context context) throws IOException, InterruptedException {
			
			final String[] callejero = censo.toString().split(SEPARATOR2);
			
			final String nombreCalle = format(callejero[1]); 
			final String codigoCalle = format(callejero[2]);
			
			int n = 5 - codigoCalle.length(); //Rellenar con ceros porque así esta representado en el fichero censoCallejero.csv
			String codigoCallePad = "";
			for (int i=0; i<n; i++) {
				codigoCallePad += "0";
			}
			codigoCallePad = codigoCallePad + codigoCalle;
			context.write(new Text(nombreCalle), new Text(tag+codigoCallePad));
		}

		private String format(String cadena) {
			return cadena.trim();
		}
		
	}	

	public static class CensoReducer extends Reducer<Text, Text, Text, Text> {
		
		private String numeroComercios, habitantes;
		
		/*hashmap donde se cargará la información recogida del fichero de censoPoblacion.csv*/
		private static Map<String,String> PoblacionMap = new HashMap<String,String>();
	   
		/* método que cargará y dejará disponible para cualquier instancia del reduce el hashmap que contendrá
		 * los datos del fichero censoPoblacion.csv
		 */
		public void setup(Context context) {
	          loadPoblacion();
	    }
		
		/* 
		 * El reducer se encarga de combinar y simplificar mediante un determinado criterio todos los datos que se obtiene del mapper
		 * En mi caso particular, el reducer se encarga de enlazar los datos del fichero censoComercios.csv y censoPoblacion.csv a
		 * través de un fichero de índices denominado censoCallejero.csv. Por lo que obtengo, para cada calle, donde al menos existe un
		 * comercio, el número de comercios totales de la calle los habitantes para esa calle, y el cociente entre el número de habitantes
		 * y el número de comercios donde se puede observar cuantos habitantes tocan por comercio. Una utilidad es que a cuanto mayor cociente
		 * más habitantes y menos comercios, por lo que podría haber una oportunidad de negocio.
		 */
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			
			int num = 0;
			int nComercios = 0;
			String habitantes = "No indice";
			String numeroComercios = "";
				
			//Iteración sobre la clave de todos los valores posibles
			for (Text nCom : value) {
				
				String currentValue = nCom.toString();
				String valueSplitted[] = currentValue.split("~");
				
				//Filtramos el número de comercios
	            if(valueSplitted[0].equals("CO")) { 
	            	numeroComercios = valueSplitted[1].trim();
	            }
	            //Filtramos por el código de la calle
	            else if(valueSplitted[0].equals("CA")) {
	            	habitantes = PoblacionMap.get(valueSplitted[1].trim());
	            }
			}
			
			//Obtengo el parámetro pasado por argumento a la aplicación y a su vez pasado al contexto
			String n = context.getConfiguration().get("nComercios");
			
			//Si existe el comercio(siempre debería existir) y el número de comercios mínimo es el indicado por mi  
			if ((numeroComercios != "") && (Integer.parseInt(numeroComercios) > Integer.parseInt(n))) {
				
				int size = key.toString().length()-40;//formato de salida
				String whiteSpace = "";//formato de salida
				String tagKey = padRight(whiteSpace, size); //formato de salida
				
				size = numeroComercios.length()-10; //formato de salida
				String tagComercios = padRight(whiteSpace, size); //formato de salida
				
				String noData = "No Data";
				size = noData.length()-10; //formato de salida
				noData += padRight (whiteSpace, size); //formato de salida
				
				
		        if(numeroComercios != null && habitantes != null) {
		        	if ((habitantes != "No indice") && (numeroComercios != "")) {
		        		size = habitantes.length()-10; //formato de salida
		        		String tagHabitantes = padRight(whiteSpace, size); //formato de salida
		        		double indice = Integer.parseInt(habitantes)/Integer.parseInt(numeroComercios);
		        		context.write(new Text(key.toString() + tagKey + numeroComercios + tagComercios), new Text(habitantes + tagHabitantes + Double.toString(indice)));
		        	} else {
		        		context.write(new Text(key.toString() + tagKey + numeroComercios + tagComercios), new Text(habitantes));
		        	}
		        }
		        
		        else if(numeroComercios == null)
		               context.write(new Text(key.toString() + tagKey + noData + tagComercios), new Text(habitantes));
		        else if(habitantes == null)
		               context.write(new Text(key.toString() + tagKey + numeroComercios + tagComercios), new Text(noData));
			}
		}
		
		//Función para el formato de salida
		public static String padRight(String s, int n) { 
			return String.format("%1$" + n + "s", s); 
		}
		
	    //Para cargar los códigos de las calles en un hash map
	    private void loadPoblacion() {
	    	
	    	String strRead;
	    	try {
	    		//Se lee el fichero pasado por parámetro
				BufferedReader reader = new BufferedReader(new FileReader("censoPoblacion.csv"));
				while ((strRead=reader.readLine() ) != null) {
					String splitarray[] = strRead.split(",");
				    //Se añade en el hashmap los pares clave-valor del fichero (clave, código de calle y valor número de habitantes)
				    PoblacionMap.put(splitarray[0].trim(), splitarray[1].trim());   
				 }
	        } 
	    	catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } 
	    	catch( IOException e ) {
	    		e.printStackTrace();
	        }
	             
	     }

	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 5) {
			System.err.println("CensoManager required params: {input file} {output dir} {minimum shops} -files {population}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf(), "Comercios y habitantes de La Laguna");
		job.setJarByClass(CensoManager.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(CensoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		//Parametro que paso al contexto
		job.getConfiguration().set("nComercios", args[2]); //Paso el número de comercios que quiero mostrar al reducer
		
		MultipleInputs.addInputPath(job, new Path(args[0]+"censoComercios.csv"), TextInputFormat.class, ComerciosMapper.class);		
		MultipleInputs.addInputPath(job, new Path(args[0]+"censoCallejero.csv"), TextInputFormat.class, CallejeroMapper.class);
	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CensoManager(), args);
	}
}

