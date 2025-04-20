
## plan
```markdown
Sink(table=[default_catalog.default_database.CheckoutEvents], fields=[name, checkout_type, items, rowtime])
+- Calc(select=[name, CAST(_UTF-16LE'CHECKOUT':VARCHAR(2147483647) CHARACTER SET "UTF-16LE" AS VARCHAR(2147483647) CHARACTER SET "UTF-16LE") AS checkout_type, items, rowtime], where=[=(checkout_type, _UTF-16LE'CHECKOUT':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")])
   +- ProcessTableFunction(invocation=[CheckoutProcessor(TABLE(#0) PARTITION BY($1), 60, 120, DESCRIPTOR(_UTF-16LE'ts'), _UTF-16LE'cart-processor')], uid=[cart-processor], select=[name,checkout_type,items,rowtime], rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) checkout_type, (BIGINT, INTEGER) MAP items, TIMESTAMP(3) *ROWTIME* rowtime)])
      +- Exchange(distribution=[hash[name]])
         +- WatermarkAssigner(rowtime=[ts], watermark=[-(ts, 1000:INTERVAL SECOND)])
            +- TableSourceScan(table=[[default_catalog, default_database, Events]], fields=[ts, name, eventType, productId])

```
## StreamExecCalc$12
```java
      public class StreamExecCalc$12 extends org.apache.flink.table.runtime.operators.TableStreamOperator
          implements org.apache.flink.streaming.api.operators.OneInputStreamOperator {

        private final Object[] references;
        private transient org.apache.flink.table.runtime.typeutils.StringDataSerializer typeSerializer$1;
        
        private final org.apache.flink.table.data.binary.BinaryStringData str$3 = org.apache.flink.table.data.binary.BinaryStringData.fromString("CHECKOUT");
                   
        private transient org.apache.flink.table.runtime.typeutils.MapDataSerializer typeSerializer$9;
        org.apache.flink.table.data.BoxedWrapperRowData out = new org.apache.flink.table.data.BoxedWrapperRowData(4);
        private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement = new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

        public StreamExecCalc$12(
            Object[] references,
            org.apache.flink.streaming.runtime.tasks.StreamTask task,
            org.apache.flink.streaming.api.graph.StreamConfig config,
            org.apache.flink.streaming.api.operators.Output output,
            org.apache.flink.streaming.runtime.tasks.ProcessingTimeService processingTimeService) throws Exception {
          this.references = references;
          typeSerializer$1 = (((org.apache.flink.table.runtime.typeutils.StringDataSerializer) references[0]));
          typeSerializer$9 = (((org.apache.flink.table.runtime.typeutils.MapDataSerializer) references[1]));
          this.setup(task, config, output);
          if (this instanceof org.apache.flink.streaming.api.operators.AbstractStreamOperator) {
            ((org.apache.flink.streaming.api.operators.AbstractStreamOperator) this)
              .setProcessingTimeService(processingTimeService);
          }
        }

        @Override
        public void open() throws Exception {
          super.open();
          
        }

        @Override
        public void processElement(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
          org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) element.getValue();
          
          org.apache.flink.table.data.binary.BinaryStringData field$0;
          boolean isNull$0;
          org.apache.flink.table.data.binary.BinaryStringData field$2;
          boolean isNull$4;
          boolean result$5;
          org.apache.flink.table.data.binary.BinaryStringData field$6;
          boolean isNull$6;
          org.apache.flink.table.data.binary.BinaryStringData field$7;
          org.apache.flink.table.data.MapData field$8;
          boolean isNull$8;
          org.apache.flink.table.data.MapData field$10;
          org.apache.flink.table.data.TimestampData field$11;
          boolean isNull$11;
          
          
          
          isNull$0 = in1.isNullAt(1);
          field$0 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
          if (!isNull$0) {
            field$0 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
          }
          field$2 = field$0;
          if (!isNull$0) {
            field$2 = (org.apache.flink.table.data.binary.BinaryStringData) (typeSerializer$1.copy(field$2));
          }
                  
          
          
          
          isNull$4 = isNull$0 || false;
          result$5 = false;
          if (!isNull$4) {
            
          
          result$5 = field$2.equals(((org.apache.flink.table.data.binary.BinaryStringData) str$3));
          
            
          }
          
          if (result$5) {
            
          isNull$6 = in1.isNullAt(0);
          field$6 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
          if (!isNull$6) {
            field$6 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(0));
          }
          field$7 = field$6;
          if (!isNull$6) {
            field$7 = (org.apache.flink.table.data.binary.BinaryStringData) (typeSerializer$1.copy(field$7));
          }
                  
          
          isNull$8 = in1.isNullAt(2);
          field$8 = null;
          if (!isNull$8) {
            field$8 = in1.getMap(2);
          }
          field$10 = field$8;
          if (!isNull$8) {
            field$10 = (org.apache.flink.table.data.MapData) (typeSerializer$9.copy(field$10));
          }
                  
          isNull$11 = in1.isNullAt(3);
          field$11 = null;
          if (!isNull$11) {
            field$11 = in1.getTimestamp(3, 3);
          }
            
          out.setRowKind(in1.getRowKind());
          
          
          
          
          if (isNull$6) {
            out.setNullAt(0);
          } else {
            out.setNonPrimitiveValue(0, field$7);
          }
                    
          
          
           // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
           
           // --- End cast section
                         
          if (false) {
            out.setNullAt(1);
          } else {
            out.setNonPrimitiveValue(1, ((org.apache.flink.table.data.binary.BinaryStringData) str$3));
          }
                    
          
          
          if (isNull$8) {
            out.setNullAt(2);
          } else {
            out.setNonPrimitiveValue(2, field$10);
          }
                    
          
          
          out.setNonPrimitiveValue(3, field$11);
                   
                  
          output.collect(outElement.replace(out));
          
          }
          
        }

        

        @Override
        public void finish() throws Exception {
            
            super.finish();
        }

        @Override
        public void close() throws Exception {
           super.close();
           
        }

        
      }
```

## ProcessTableRunner$7
```java

      public final class ProcessTableRunner$7
          extends org.apache.flink.table.runtime.generated.ProcessTableRunner {

        private transient org.apache.flink.table.examples.java.my.ShoppingCartExample$CheckoutProcessor function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor;
        private transient org.apache.flink.table.data.conversion.StructuredObjectConverter converter$1;
        private transient org.apache.flink.table.data.conversion.RowRowConverter converter$3;
        private TableFunctionResultConverterCollector$4 resultConverterCollector$5 = null;
        private transient org.apache.flink.table.data.conversion.RowRowConverter converter$6;

        public ProcessTableRunner$7(Object[] references) throws Exception {
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor = (((org.apache.flink.table.examples.java.my.ShoppingCartExample$CheckoutProcessor) references[0]));
          converter$1 = (((org.apache.flink.table.data.conversion.StructuredObjectConverter) references[1]));
          converter$3 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[2]));
          converter$6 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[3]));
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
          
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
                 
          
          converter$1.open(getRuntimeContext().getUserCodeClassLoader());
                     
          
          converter$3.open(getRuntimeContext().getUserCodeClassLoader());
                     
          
          resultConverterCollector$5 = new TableFunctionResultConverterCollector$4();
          resultConverterCollector$5.setRuntimeContext(getRuntimeContext());
          resultConverterCollector$5.open(new org.apache.flink.api.common.functions.DefaultOpenContext());
          
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor.setCollector(resultConverterCollector$5);
          
          converter$6.open(getRuntimeContext().getUserCodeClassLoader());
                     
        }

        @Override
        public void callEval() throws Exception {
          
          
          resultConverterCollector$5.setCollector(evalCollector);
          
          final org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart externalState$0;
          if (stateToFunction[0] == null) {
            externalState$0 = new org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart();
          } else {
            externalState$0 = (org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart) converter$1.toExternal((org.apache.flink.table.data.RowData) stateToFunction[0]);
          }
          
          
          
          
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor.eval(runnerContext, externalState$0, inputIndex != 0 ? null : ((org.apache.flink.types.Row) converter$6.toExternal((org.apache.flink.table.data.RowData) inputRow)), false ? null : ((java.lang.Integer) ((int) 60)), false ? null : ((java.lang.Integer) ((int) 120)));
          stateFromFunction[0] = stateCleared[0] ? null : (org.apache.flink.table.data.RowData) converter$1.toInternalOrNull((org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart) externalState$0);
          
          stateFromFunction[0] = stateCleared[0] ? null : (org.apache.flink.table.data.RowData) converter$1.toInternalOrNull((org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart) externalState$0);
        }

        @Override
        public void callOnTimer() throws Exception {
          
          
          resultConverterCollector$5.setCollector(onTimerCollector);
          
          final org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart externalState$0;
          if (stateToFunction[0] == null) {
            externalState$0 = new org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart();
          } else {
            externalState$0 = (org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart) converter$1.toExternal((org.apache.flink.table.data.RowData) stateToFunction[0]);
          }
          
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor.onTimer(runnerOnTimerContext, externalState$0);
          stateFromFunction[0] = stateCleared[0] ? null : (org.apache.flink.table.data.RowData) converter$1.toInternalOrNull((org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart) externalState$0);
          
        }

        @Override
        public void close() throws Exception {
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor.close();
        }


      public class TableFunctionResultConverterCollector$4 extends org.apache.flink.table.runtime.collector.WrappingCollector {

        

        public TableFunctionResultConverterCollector$4() throws Exception {
          
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
          
        }

        @Override
        public void collect(Object record) throws Exception {
          org.apache.flink.table.data.RowData externalResult$2 = (org.apache.flink.table.data.RowData) (org.apache.flink.table.data.RowData) converter$3.toInternalOrNull((org.apache.flink.types.Row) record);
          
          
          
          
          if (externalResult$2 != null) {
            outputResult(externalResult$2);
          }
          
        }

        @Override
        public void close() {
          try {
            
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    
      }
```


```java

      public class StateHashCode$8 implements org.apache.flink.table.runtime.generated.HashFunction {

        private transient org.apache.flink.table.examples.java.my.ShoppingCartExample$CheckoutProcessor function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor;
        private transient org.apache.flink.table.data.conversion.StructuredObjectConverter converter$1;
        private transient org.apache.flink.table.data.conversion.RowRowConverter converter$3;
        private TableFunctionResultConverterCollector$4 resultConverterCollector$5 = null;
        private transient org.apache.flink.table.data.conversion.RowRowConverter converter$6;
        private transient java.lang.Object[] subRefs$22;
        org.apache.flink.table.runtime.generated.HashFunction hashFunc$23;

        public StateHashCode$8(Object[] references) throws Exception {
          function_org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor = (((org.apache.flink.table.examples.java.my.ShoppingCartExample$CheckoutProcessor) references[0]));
          converter$1 = (((org.apache.flink.table.data.conversion.StructuredObjectConverter) references[1]));
          converter$3 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[2]));
          converter$6 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[3]));
          subRefs$22 = (((java.lang.Object[]) references[4]));
          hashFunc$23 = new SubHashMap$11(subRefs$22);
        }

        @Override
        public int hashCode(Object _in) {
          org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) _in;
          org.apache.flink.table.data.MapData field$9;
          boolean isNull$9;
          
          int hashCode$10 = 0;
          
          hashCode$10 *= 73;
          isNull$9 = in1.isNullAt(0);
          field$9 = null;
          if (!isNull$9) {
            field$9 = in1.getMap(0);
          }
          if (!isNull$9) {
           hashCode$10 += hashFunc$23.hashCode(field$9);
          }
          
          return hashCode$10;
        }

        
              public class SubHashMap$11 implements org.apache.flink.table.runtime.generated.HashFunction {
        
                
        
                public SubHashMap$11(Object[] references) throws Exception {
                  
                }
        
                @Override
                public int hashCode(Object _in) {
                  org.apache.flink.table.data.MapData in1 = (org.apache.flink.table.data.MapData) _in;
                  org.apache.flink.table.data.ArrayData keys$12 = in1.keyArray();
                  org.apache.flink.table.data.ArrayData values$13 = in1.valueArray();
        
                  int keyHashCode$18 = 0;
                  int valueHashCode$19 = 0;
                  int hashCode$20 = 0;
                  
                  // This is inspired by hive & presto
                  for (int i$21 = 0; i$21 < in1.size(); i$21++) {
                    boolean keyIsNull$14 = keys$12.isNullAt(i$21);
                    keyHashCode$18 = 0;
                    if (!keyIsNull$14) {
                      long keyFieldTerm$15 = keys$12.getLong(i$21);
                      keyHashCode$18 = java.lang.Long.hashCode(keyFieldTerm$15);
                    }
        
                    boolean valueIsNull$16 = values$13.isNullAt(i$21);
                    valueHashCode$19 = 0;
                    if(!valueIsNull$16) {
                      int valueFieldTerm$17 = values$13.getInt(i$21);
                      valueHashCode$19 = java.lang.Integer.hashCode(valueFieldTerm$17);
                    }
                    
                    hashCode$20 += keyHashCode$18 ^ valueHashCode$19;
                  }
        
                  return hashCode$20;
                }
        
                
              }
            
        
              public class TableFunctionResultConverterCollector$4 extends org.apache.flink.table.runtime.collector.WrappingCollector {
        
                
        
                public TableFunctionResultConverterCollector$4() throws Exception {
                  
                }
        
                @Override
                public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
                  
                }
        
                @Override
                public void collect(Object record) throws Exception {
                  org.apache.flink.table.data.RowData externalResult$2 = (org.apache.flink.table.data.RowData) (org.apache.flink.table.data.RowData) converter$3.toInternalOrNull((org.apache.flink.types.Row) record);
                  
                  
                  
                  
                  if (externalResult$2 != null) {
                    outputResult(externalResult$2);
                  }
                  
                }
        
                @Override
                public void close() {
                  try {
                    
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }
              }
            
      }
```


```java
public class org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor$ShoppingCart$0$Converter implements org.apache.flink.table.data.conversion.DataStructureConverter {
    private final org.apache.flink.table.data.RowData.FieldGetter[] fieldGetters;
    private final org.apache.flink.table.data.conversion.DataStructureConverter[] fieldConverters;
    public org$apache$flink$table$examples$java$my$ShoppingCartExample$CheckoutProcessor$ShoppingCart$0$Converter(org.apache.flink.table.data.RowData.FieldGetter[] fieldGetters, org.apache.flink.table.data.conversion.DataStructureConverter[] fieldConverters) {
        this.fieldGetters = fieldGetters;
        this.fieldConverters = fieldConverters;
    }
    public java.lang.Object toInternal(java.lang.Object o) {
        final org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart external = (org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart) o;
        final org.apache.flink.table.data.GenericRowData genericRow = new org.apache.flink.table.data.GenericRowData(1);
        genericRow.setField(0, fieldConverters[0].toInternalOrNull(((java.util.Map) external.content)));
        return genericRow;
    }
    public java.lang.Object toExternal(java.lang.Object o) {
        final org.apache.flink.table.data.RowData internal = (org.apache.flink.table.data.RowData) o;
        final org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart structured = new org.apache.flink.table.examples.java.my.ShoppingCartExample.CheckoutProcessor.ShoppingCart();
        structured.content = ((java.util.Map) fieldConverters[0].toExternalOrNull(fieldGetters[0].getFieldOrNull(internal)));
        return structured;
    }
}
```


