## UDF Async
[UDFExample.java](UDFExample.java)

[UDF初始化流程](https://lucid.app/lucidchart/df483411-3bbf-416c-ad32-396e187731ea/edit?invitationId=inv_66946a96-7421-4051-85ca-b2d1f29d5d43&page=1tVQBjOVqmxY#)

### fixme
这里有个问题，针对`SocketSource.SocketReader`本次数据不能及时处理
`MailboxProcessor.processMail` → `createBatch` 会处理上次的遗留的ResultHandler的mail, 
本次的`processResults`会在mailbox queue中， createBatch之后会处理到batch中。
如果长时间socket没有数据，会导致queue不断增加。

### AsyncScalarFunction$5
```java
public class AsyncScalarFunction$5
    extends org.apache.flink.streaming.api.functions.async.RichAsyncFunction {
  private transient org.apache.flink.table.runtime.typeutils.StringDataSerializer typeSerializer$1;
  private transient org.apache.flink.table.examples.java.my.UDFExample$MyLengthAsyncFunction function_org$apache$flink$table$examples$java$my$UDFExample$MyLengthAsyncFunction;
  private transient org.apache.flink.table.data.conversion.StringStringConverter converter$3;
  private transient org.apache.flink.table.data.conversion.IdentityConverter converter$4;

  public AsyncScalarFunction$5(Object[] references) throws Exception {
    typeSerializer$1 = (((org.apache.flink.table.runtime.typeutils.StringDataSerializer) references[0]));
    function_org$apache$flink$table$examples$java$my$UDFExample$MyLengthAsyncFunction = (((org.apache.flink.table.examples.java.my.UDFExample$MyLengthAsyncFunction) references[1]));
    converter$3 = (((org.apache.flink.table.data.conversion.StringStringConverter) references[2]));
    converter$4 = (((org.apache.flink.table.data.conversion.IdentityConverter) references[3]));
  }
  @Override
  public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
    function_org$apache$flink$table$examples$java$my$UDFExample$MyLengthAsyncFunction.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
    converter$3.open(getRuntimeContext().getUserCodeClassLoader());
    converter$4.open(getRuntimeContext().getUserCodeClassLoader());
  }

  @Override
  public void asyncInvoke(Object _in1, org.apache.flink.streaming.api.functions.async.ResultFuture c) throws Exception {
    org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) _in1;
    org.apache.flink.table.data.binary.BinaryStringData field$0;
    boolean isNull$0;
    org.apache.flink.table.data.binary.BinaryStringData field$2;
    isNull$0 = in1.isNullAt(0);
    field$0 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    if (!isNull$0) {
      field$0 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(0));
    }
    field$2 = field$0;
    if (!isNull$0) {
      field$2 = (org.apache.flink.table.data.binary.BinaryStringData) (typeSerializer$1.copy(field$2));
    }
    final org.apache.flink.table.runtime.operators.calc.async.DelegatingAsyncResultFuture f 
        = new org.apache.flink.table.runtime.operators.calc.async.DelegatingAsyncResultFuture(c);
    final org.apache.flink.types.RowKind rowKind = in1.getRowKind();
    try {
      java.util.function.Function<Object, org.apache.flink.table.data.GenericRowData> outputFactory = 
        new java.util.function.Function<Object, org.apache.flink.table.data.GenericRowData>() {
        @Override
        public org.apache.flink.table.data.GenericRowData apply(Object resultObject) {
          final org.apache.flink.table.data.GenericRowData out = new org.apache.flink.table.data.GenericRowData(2);
          out.setRowKind(rowKind);
          out.setField(0, f.getSynchronousResult(0));
          out.setField(1, resultObject);
          return out;
        }
      };
      f.setOutputFactory(outputFactory);
      f.addSynchronousResult(field$2);
      if (isNull$0) {
        f.createAsyncFuture(converter$4).complete(null);
      } else {
        function_org$apache$flink$table$examples$java$my$UDFExample$MyLengthAsyncFunction.eval(
          f.createAsyncFuture(converter$4),
          isNull$0 ? null : ((java.lang.String) converter$3.toExternal((org.apache.flink.table.data.binary.BinaryStringData) field$2)));
      }
    } catch (Throwable e) {
      c.completeExceptionally(e);
    }
  }
  @Override
  public void close() throws Exception {
    function_org$apache$flink$table$examples$java$my$UDFExample$MyLengthAsyncFunction.close();
  }
}
```

## [UDTFExample2.java](UDTFExample2.java)
### optimize plan:
```markdown
Sink(table=[default_catalog.default_database.MySink], fields=[newWord, newLength])
+- GroupAggregate(groupBy=[newWord], select=[newWord, SUM(newLength) AS newLength])
   +- Exchange(distribution=[hash[newWord]])
      +- Calc(select=[word AS newWord, length AS newLength])
         +- Correlate(invocation=[SplitFunction($cor0.myField)], correlate=[table(SplitFunction($cor0.myField))], select=[myField,word,length], rowType=[RecordType(VARCHAR(2147483647) myField, VARCHAR(2147483647) word, INTEGER length)], joinType=[LEFT])
            +- TableSourceScan(table=[[default_catalog, default_database, MyTable]], fields=[myField])
```

#### Calc
@see org.apache.calcite.rel.core.Calc, 
`StreamExecCalc` 的作用是处理流式数据中的计算操作，包括UDF，Projection，Filter等。
它继承自 `CommonExecCalc`，并实现了 `StreamExecNode` 接口。
`StreamExecCalc` 主要用于在流式数据处理中执行投影（projection）和过滤（filter）操作。

OneInputTransformation:

Operator: GeneratedOperator(StreamExecCalc$4)


```java

  public class StreamExecCalc$4 extends org.apache.flink.table.runtime.operators.TableStreamOperator
      implements org.apache.flink.streaming.api.operators.OneInputStreamOperator {
    private final Object[] references;
    private transient org.apache.flink.table.runtime.typeutils.StringDataSerializer typeSerializer$1;
    org.apache.flink.table.data.BoxedWrapperRowData out = new org.apache.flink.table.data.BoxedWrapperRowData(2);
    private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement = new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

    public StreamExecCalc$4(
        Object[] references,
        org.apache.flink.streaming.runtime.tasks.StreamTask task,
        org.apache.flink.streaming.api.graph.StreamConfig config,
        org.apache.flink.streaming.api.operators.Output output,
        org.apache.flink.streaming.runtime.tasks.ProcessingTimeService processingTimeService) throws Exception {
      this.references = references;
      typeSerializer$1 = (((org.apache.flink.table.runtime.typeutils.StringDataSerializer) references[0]));
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
      int field$3;
      boolean isNull$3;
      isNull$3 = in1.isNullAt(2);
      field$3 = -1;
      if (!isNull$3) {
        field$3 = in1.getInt(2);
      }
      
      isNull$0 = in1.isNullAt(1);
      field$0 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
      if (!isNull$0) {
        field$0 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
      }
      field$2 = field$0;
      if (!isNull$0) {
        field$2 = (org.apache.flink.table.data.binary.BinaryStringData) (typeSerializer$1.copy(field$2));
      }
      out.setRowKind(in1.getRowKind());
      if (isNull$0) {
        out.setNullAt(0);
      } else {
        out.setNonPrimitiveValue(0, field$2);
      }
      if (isNull$3) {
        out.setNullAt(1);
      } else {
        out.setInt(1, field$3);
      }
      output.collect(outElement.replace(out));
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


#### Correlate
@see `org.apache.calcite.rel.core.Correlate`, `NestedLoops` in Physical operation.
`StreamExecCorrelate` -> `LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE`
这里会调用 `SplitFunction` 函数，该方法会flatmap collect:
`UDFExample2$SplitFunction.evel` -> 
`TableFunctionResultConverterCollector.collect` -> 
`TableFunctionCollector.collect` -> 
`TableFunctionCollector.outputResult` -> `org.apache.flink.streaming.api.operators.Output output`

OneInputTransformation:

Operator: GeneratedOperator(StreamExecCorrelate$12)
```java

      public class StreamExecCorrelate$12 extends org.apache.flink.table.runtime.operators.TableStreamOperator
          implements org.apache.flink.streaming.api.operators.OneInputStreamOperator {

        private final Object[] references;
        private TableFunctionCollector$2 correlateCollector$0 = null;
        private transient org.apache.flink.table.examples.java.my.UDFExample2$SplitFunction function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction;
        private transient org.apache.flink.table.data.conversion.StringStringConverter converter$5;
        private transient org.apache.flink.table.data.conversion.RowRowConverter converter$7;
        private TableFunctionResultConverterCollector$8 resultConverterCollector$9 = null;
        org.apache.flink.table.data.utils.JoinedRowData joinedRow$10 = new org.apache.flink.table.data.utils.JoinedRowData();
        org.apache.flink.table.data.GenericRowData nullRow$11 = new org.apache.flink.table.data.GenericRowData(2);
        private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement = new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

        public StreamExecCorrelate$12(
            Object[] references,
            org.apache.flink.streaming.runtime.tasks.StreamTask task,
            org.apache.flink.streaming.api.graph.StreamConfig config,
            org.apache.flink.streaming.api.operators.Output output,
            org.apache.flink.streaming.runtime.tasks.ProcessingTimeService processingTimeService) throws Exception {
          this.references = references;
          function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction = (((org.apache.flink.table.examples.java.my.UDFExample2$SplitFunction) references[0]));
          converter$5 = (((org.apache.flink.table.data.conversion.StringStringConverter) references[1]));
          converter$7 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[2]));
          this.setup(task, config, output);
          if (this instanceof org.apache.flink.streaming.api.operators.AbstractStreamOperator) {
            ((org.apache.flink.streaming.api.operators.AbstractStreamOperator) this).setProcessingTimeService(processingTimeService);
          }
        }

        @Override
        public void open() throws Exception {
          super.open();
          correlateCollector$0 = new TableFunctionCollector$2();
          correlateCollector$0.setRuntimeContext(getRuntimeContext());
          correlateCollector$0.open(new org.apache.flink.api.common.functions.DefaultOpenContext());
          function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
          converter$5.open(getRuntimeContext().getUserCodeClassLoader());
          converter$7.open(getRuntimeContext().getUserCodeClassLoader());
          resultConverterCollector$9 = new TableFunctionResultConverterCollector$8();
          resultConverterCollector$9.setRuntimeContext(getRuntimeContext());
          resultConverterCollector$9.open(new org.apache.flink.api.common.functions.DefaultOpenContext());
          function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction.setCollector(resultConverterCollector$9);
          correlateCollector$0.setCollector(new org.apache.flink.table.runtime.util.StreamRecordCollector(output));
          resultConverterCollector$9.setCollector(correlateCollector$0);
        }

        @Override
        public void processElement(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
          org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) element.getValue();
          org.apache.flink.table.data.binary.BinaryStringData field$3;
          boolean isNull$3;
          org.apache.flink.table.data.binary.BinaryStringData result$4;
          boolean isNull$4;
          correlateCollector$0.setInput(in1);
          correlateCollector$0.reset();
          if (false) {
            result$4 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            isNull$4 = true;
          } else {
            isNull$3 = in1.isNullAt(0);
            field$3 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            if (!isNull$3) {
              field$3 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(0));
            }
            result$4 = field$3;
            isNull$4 = isNull$3;
          }
          function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction.eval(isNull$4 ? null : ((java.lang.String) converter$5.toExternal((org.apache.flink.table.data.binary.BinaryStringData) result$4)));
          boolean hasOutput = correlateCollector$0.isCollected();
          if (!hasOutput) {
            joinedRow$10.replace(in1, nullRow$11);
            joinedRow$10.setRowKind(in1.getRowKind());
            correlateCollector$0.outputResult(joinedRow$10);
          }
        }

        @Override
        public void finish() throws Exception {
            function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction.finish();
            super.finish();
        }

        @Override
        public void close() throws Exception {
           super.close();
           function_org$apache$flink$table$examples$java$my$UDFExample2$SplitFunction.close();
        }
        
        public class TableFunctionResultConverterCollector$8 extends org.apache.flink.table.runtime.collector.WrappingCollector {
          public TableFunctionResultConverterCollector$8() throws Exception {}
          @Override
          public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {}
          @Override
          public void collect(Object record) throws Exception {
            org.apache.flink.table.data.RowData externalResult$6 = (org.apache.flink.table.data.RowData) (org.apache.flink.table.data.RowData) converter$7.toInternalOrNull((org.apache.flink.types.Row) record);
            if (externalResult$6 != null) {
              outputResult(externalResult$6);
            }
          }
          @Override
          public void close() {}
        }
        
        public class TableFunctionCollector$2 extends org.apache.flink.table.runtime.collector.TableFunctionCollector {
          org.apache.flink.table.data.utils.JoinedRowData joinedRow$1 = new org.apache.flink.table.data.utils.JoinedRowData();
          public TableFunctionCollector$2() throws Exception {}
          @Override
          public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {}
          @Override
          public void collect(Object record) throws Exception {
            org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) getInput();
            org.apache.flink.table.data.RowData in2 = (org.apache.flink.table.data.RowData) record;
            joinedRow$1.replace(in1, in2);
            joinedRow$1.setRowKind(in1.getRowKind());
            outputResult(joinedRow$1);
          }
          @Override
          public void close() {}
        }
      }
    
```

## [UDTFExample3.java](UDTFExample3.java)
异步支持: FLIP-498 FLIP-313 还在讨论，没有实现

# Table-Valued Function
## TUMBLE 滚动窗口
### [Exchange global window](WVFExample.java)
```markdown
 LogicalSink(table=[default_catalog.default_database.MySink], fields=[window_start, window_end, total_score])
+- LogicalAggregate(group=[{0, 1}], total_score=[SUM($2)])
   +- LogicalProject(exprs=[[$3, $4, $1]])
      +- LogicalTableFunctionScan(invocation=[TUMBLE(TABLE(#0), DESCRIPTOR($2), 5000:INTERVAL SECOND)], rowType=[RecordType(VARCHAR(2147483647) name, INTEGER score, TIMESTAMP(3) *ROWTIME* event_time, TIMESTAMP(3) window_start, TIMESTAMP(3) window_end, TIMESTAMP(3) *ROWTIME* window_time)])
         +- LogicalProject(inputs=[0..2])
            +- LogicalWatermarkAssigner(rowtime=[event_time], watermark=[-($2, 2000:INTERVAL SECOND)])
               +- LogicalTableScan(table=[[default_catalog, default_database, t]])
```

```markdown
StreamPhysicalSink(table=[default_catalog.default_database.MySink], fields=[window_start, window_end, total_score])
    StreamPhysicalCalc(select=[window_start, window_end, total_score])
        StreamPhysicalGlobalWindowAggregate(window=[TUMBLE(slice_end=[$slice_end], size=[5 s])], select=[SUM(sum$0) AS total_score, start('w$) AS window_start, end('w$) AS window_end])
            StreamPhysicalExchange(distribution=[single])
                StreamPhysicalLocalWindowAggregate(window=[TUMBLE(time_col=[event_time], size=[5 s])], select=[SUM(score) AS sum$0, slice_end('w$) AS $slice_end])
                    StreamPhysicalCalc(select=[score, event_time])
                        StreamPhysicalWatermarkAssigner(rowtime=[event_time], watermark=[-(event_time, 2000:INTERVAL SECOND)])
                            StreamPhysicalTableSourceScan(table=[[default_catalog, default_database, t]], fields=[name, score, event_time])
```

```markdown
Sink(table=[default_catalog.default_database.MySink], fields=[window_start, window_end, total_score])
+- Calc(select=[window_start, window_end, total_score])
   +- GlobalWindowAggregate(window=[TUMBLE(slice_end=[$slice_end], size=[5 s])], select=[SUM(sum$0) AS total_score, start('w$) AS window_start, end('w$) AS window_end])
      +- Exchange(distribution=[single])
         +- LocalWindowAggregate(window=[TUMBLE(time_col=[event_time], size=[5 s])], select=[SUM(score) AS sum$0, slice_end('w$) AS $slice_end])
            +- Calc(select=[score, event_time])
               +- WatermarkAssigner(rowtime=[event_time], watermark=[-(event_time, 2000:INTERVAL SECOND)])
                  +- TableSourceScan(table=[[default_catalog, default_database, t]], fields=[name, score, event_time])
```

LocalWindowAggsHandler$15
```java
public final class LocalWindowAggsHandler$15 implements org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction<java.lang.Long> {
  private transient org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.SlicedUnsharedSliceAssigner windowAssigner$0;
  int agg0_sum;
  boolean agg0_sumIsNull;
  org.apache.flink.table.data.GenericRowData acc$1 = new org.apache.flink.table.data.GenericRowData(1);
  org.apache.flink.table.data.GenericRowData acc$2 = new org.apache.flink.table.data.GenericRowData(1);
  org.apache.flink.table.data.GenericRowData aggValue$14 = new org.apache.flink.table.data.GenericRowData(3);
  private org.apache.flink.table.runtime.dataview.StateDataViewStore store;
  private java.lang.Long namespace;
  public LocalWindowAggsHandler$15(Object[] references) throws Exception {
    windowAssigner$0 = (((org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.SlicedUnsharedSliceAssigner) references[0]));
  }
  private org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {
    return store.getRuntimeContext();
  }
  @Override
  public void open(org.apache.flink.table.runtime.dataview.StateDataViewStore store) throws Exception {
    this.store = store;
  }
  @Override
  public void accumulate(org.apache.flink.table.data.RowData accInput) throws Exception {
    int field$4;
    boolean isNull$4;
    boolean isNull$5;
    int result$6;
    isNull$4 = accInput.isNullAt(0);
    field$4 = -1;
    if (!isNull$4) {
      field$4 = accInput.getInt(0);
    }
    int result$8 = -1;
    boolean isNull$8;
    if (isNull$4) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$8 = agg0_sumIsNull;
      if (!isNull$8) {
        result$8 = agg0_sum;
      }
    }
    else {
      int result$7 = -1;
      boolean isNull$7;
      if (agg0_sumIsNull) {
       // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
       // --- End cast section
        isNull$7 = isNull$4;
        if (!isNull$7) {
          result$7 = field$4;
        }
      }
      else {
      isNull$5 = agg0_sumIsNull || isNull$4;
      result$6 = -1;
      if (!isNull$5) {
      result$6 = (int) (agg0_sum + field$4);
    }
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$7 = isNull$5;
      if (!isNull$7) {
        result$7 = result$6;
      }
    }
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$8 = isNull$7;
      if (!isNull$8) {
        result$8 = result$7;
      }
    }
    agg0_sum = result$8;
    agg0_sumIsNull = isNull$8;
  }
  @Override
  public void retract(org.apache.flink.table.data.RowData retractInput) throws Exception {
    throw new java.lang.RuntimeException("This function not require retract method, but the retract method is called.");
  }

  @Override
  public void merge(Object ns, org.apache.flink.table.data.RowData otherAcc) throws Exception {
    namespace = (java.lang.Long) ns;
    int field$9;
    boolean isNull$9;
    boolean isNull$10;
    int result$11;
    isNull$9 = otherAcc.isNullAt(0);
    field$9 = -1;
    if (!isNull$9) {
      field$9 = otherAcc.getInt(0);
    }
    int result$13 = -1;
    boolean isNull$13;
    if (isNull$9) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$13 = agg0_sumIsNull;
      if (!isNull$13) {
        result$13 = agg0_sum;
      }
    }
    else {
      int result$12 = -1;
      boolean isNull$12;
      if (agg0_sumIsNull) {
       // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
       // --- End cast section
        isNull$12 = isNull$9;
        if (!isNull$12) {
          result$12 = field$9;
        }
      }
      else {
        isNull$10 = agg0_sumIsNull || isNull$9;
        result$11 = -1;
        if (!isNull$10) {
          result$11 = (int) (agg0_sum + field$9);
        }
       // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
       // --- End cast section
        isNull$12 = isNull$10;
        if (!isNull$12) {
          result$12 = result$11;
        }
      }
      // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
      // --- End cast section
      isNull$13 = isNull$12;
      if (!isNull$13) {
        result$13 = result$12;
      }
    }
    agg0_sum = result$13;
    agg0_sumIsNull = isNull$13;
  }

  @Override
  public void setAccumulators(Object ns, org.apache.flink.table.data.RowData acc) throws Exception {
    namespace = (java.lang.Long) ns;
    int field$3;
    boolean isNull$3;
    isNull$3 = acc.isNullAt(0);
    field$3 = -1;
    if (!isNull$3) {
      field$3 = acc.getInt(0);
    }
    agg0_sum = field$3;;
    agg0_sumIsNull = isNull$3;
  }

  @Override
  public org.apache.flink.table.data.RowData getAccumulators() throws Exception {
    acc$2 = new org.apache.flink.table.data.GenericRowData(1);
    if (agg0_sumIsNull) {
      acc$2.setField(0, null);
    } else {
      acc$2.setField(0, agg0_sum);
    }
    return acc$2;
  }

  @Override
  public org.apache.flink.table.data.RowData createAccumulators() throws Exception {
    acc$1 = new org.apache.flink.table.data.GenericRowData(1);
    if (true) {
      acc$1.setField(0, null);
    } else {
      acc$1.setField(0, ((int) -1));
    }
    return acc$1;
  }

  @Override
  public org.apache.flink.table.data.RowData getValue(Object ns) throws Exception {
    namespace = (java.lang.Long) ns;
    aggValue$14 = new org.apache.flink.table.data.GenericRowData(3);
    if (agg0_sumIsNull) {
      aggValue$14.setField(0, null);
    } else {
      aggValue$14.setField(0, agg0_sum);
    }
    aggValue$14.setField(1, org.apache.flink.table.data.TimestampData.fromEpochMillis(windowAssigner$0.getWindowStart(namespace)));
    aggValue$14.setField(2, org.apache.flink.table.data.TimestampData.fromEpochMillis(namespace));
    return aggValue$14;
  }
  @Override
  public void cleanup(Object ns) throws Exception {
    namespace = (java.lang.Long) ns;
  }
  @Override
  public void close() throws Exception {}
}
```


GlobalWindowAggsHandler$15
```java
public final class GlobalWindowAggsHandler$15 implements org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction<java.lang.Long> {
    private transient org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.SlicedUnsharedSliceAssigner windowAssigner$0;
    int agg0_sum;
    boolean agg0_sumIsNull;
    org.apache.flink.table.data.GenericRowData acc$1 = new org.apache.flink.table.data.GenericRowData(1);
    org.apache.flink.table.data.GenericRowData acc$2 = new org.apache.flink.table.data.GenericRowData(1);
    org.apache.flink.table.data.GenericRowData aggValue$14 = new org.apache.flink.table.data.GenericRowData(3);
    private org.apache.flink.table.runtime.dataview.StateDataViewStore store;
    private java.lang.Long namespace;
    public GlobalWindowAggsHandler$15(Object[] references) throws Exception {
        windowAssigner$0 = (((org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.SlicedUnsharedSliceAssigner) references[0]));
    }
    private org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {
        return store.getRuntimeContext();
    }
    @Override
    public void open(org.apache.flink.table.runtime.dataview.StateDataViewStore store) throws Exception {
        this.store = store;
    }
    @Override
    public void accumulate(org.apache.flink.table.data.RowData accInput) throws Exception {
        int field$4;
        boolean isNull$4;
        boolean isNull$5;
        int result$6;
        isNull$4 = accInput.isNullAt(0);
        field$4 = -1;
        if (!isNull$4) {
            field$4 = accInput.getInt(0);
        }
        int result$8 = -1;
        boolean isNull$8;
        if (isNull$4) {
            // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
            // --- End cast section
            isNull$8 = agg0_sumIsNull;
            if (!isNull$8) {
                result$8 = agg0_sum;
            }
        }
        else {
            int result$7 = -1;
            boolean isNull$7;
            if (agg0_sumIsNull) {
                // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
                // --- End cast section
                isNull$7 = isNull$4;
                if (!isNull$7) {
                    result$7 = field$4;
                }
            }
            else {
                isNull$5 = agg0_sumIsNull || isNull$4;
                result$6 = -1;
                if (!isNull$5) {
                    result$6 = (int) (agg0_sum + field$4);
                }
                // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
                // --- End cast section
                isNull$7 = isNull$5;
                if (!isNull$7) {
                    result$7 = result$6;
                }
            }
            // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
            // --- End cast section
            isNull$8 = isNull$7;
            if (!isNull$8) {
                result$8 = result$7;
            }
        }
        agg0_sum = result$8;
        agg0_sumIsNull = isNull$8;
    }

    @Override
    public void retract(org.apache.flink.table.data.RowData retractInput) throws Exception {
        throw new java.lang.RuntimeException("This function not require retract method, but the retract method is called.");
    }

    @Override
    public void merge(Object ns, org.apache.flink.table.data.RowData otherAcc) throws Exception {
        namespace = (java.lang.Long) ns;
        int field$9;
        boolean isNull$9;
        boolean isNull$10;
        int result$11;
        isNull$9 = otherAcc.isNullAt(0);
        field$9 = -1;
        if (!isNull$9) {
            field$9 = otherAcc.getInt(0);
        }
        int result$13 = -1;
        boolean isNull$13;
        if (isNull$9) {
            // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
            // --- End cast section
            isNull$13 = agg0_sumIsNull;
            if (!isNull$13) {
                result$13 = agg0_sum;
            }
        }
        else {
            int result$12 = -1;
            boolean isNull$12;
            if (agg0_sumIsNull) {
                // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
                // --- End cast section
                isNull$12 = isNull$9;
                if (!isNull$12) {
                    result$12 = field$9;
                }
            }
            else {
                isNull$10 = agg0_sumIsNull || isNull$9;
                result$11 = -1;
                if (!isNull$10) {
                    result$11 = (int) (agg0_sum + field$9);
                }
                // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
                // --- End cast section
                isNull$12 = isNull$10;
                if (!isNull$12) {
                    result$12 = result$11;
                }
            }
            // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
            // --- End cast section
            isNull$13 = isNull$12;
            if (!isNull$13) {
                result$13 = result$12;
            }
        }
        agg0_sum = result$13;;
        agg0_sumIsNull = isNull$13;
    }

    @Override
    public void setAccumulators(Object ns, org.apache.flink.table.data.RowData acc)
            throws Exception {
        namespace = (java.lang.Long) ns;
        int field$3;
        boolean isNull$3;
        isNull$3 = acc.isNullAt(0);
        field$3 = -1;
        if (!isNull$3) {
            field$3 = acc.getInt(0);
        }
        agg0_sum = field$3;;
        agg0_sumIsNull = isNull$3;
    }

    @Override
    public org.apache.flink.table.data.RowData getAccumulators() throws Exception {
        acc$2 = new org.apache.flink.table.data.GenericRowData(1);
        if (agg0_sumIsNull) {
            acc$2.setField(0, null);
        } else {
            acc$2.setField(0, agg0_sum);
        }
        return acc$2;
    }
    @Override
    public org.apache.flink.table.data.RowData createAccumulators() throws Exception {
        acc$1 = new org.apache.flink.table.data.GenericRowData(1);
        if (true) {
            acc$1.setField(0, null);
        } else {
            acc$1.setField(0, ((int) -1));
        }
        return acc$1;
    }

    @Override
    public org.apache.flink.table.data.RowData getValue(Object ns) throws Exception {
        namespace = (java.lang.Long) ns;
        aggValue$14 = new org.apache.flink.table.data.GenericRowData(3);
        if (agg0_sumIsNull) {
            aggValue$14.setField(0, null);
        } else {
            aggValue$14.setField(0, agg0_sum);
        }
        aggValue$14.setField(1, org.apache.flink.table.data.TimestampData.fromEpochMillis(windowAssigner$0.getWindowStart(namespace)));
        aggValue$14.setField(2, org.apache.flink.table.data.TimestampData.fromEpochMillis(namespace));
        return aggValue$14;
    }

    @Override
    public void cleanup(Object ns) throws Exception {
        namespace = (java.lang.Long) ns;
    }

    @Override
    public void close() throws Exception {}
}
```


StateWindowAggsHandler$15
```java
public final class StateWindowAggsHandler$15
  implements org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction<java.lang.Long> {
  private transient org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.SlicedUnsharedSliceAssigner windowAssigner$0;
  int agg0_sum;
  boolean agg0_sumIsNull;
  org.apache.flink.table.data.GenericRowData acc$1 = new org.apache.flink.table.data.GenericRowData(1);
  org.apache.flink.table.data.GenericRowData acc$2 = new org.apache.flink.table.data.GenericRowData(1);
  org.apache.flink.table.data.GenericRowData aggValue$14 = new org.apache.flink.table.data.GenericRowData(3);
  private org.apache.flink.table.runtime.dataview.StateDataViewStore store;
  private java.lang.Long namespace;
  public StateWindowAggsHandler$15(Object[] references) throws Exception {
    windowAssigner$0 = (((org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.SlicedUnsharedSliceAssigner) references[0]));
  }
  private org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {
    return store.getRuntimeContext();
  }
  @Override
  public void open(org.apache.flink.table.runtime.dataview.StateDataViewStore store) throws Exception {
    this.store = store;
  }
  @Override
  public void accumulate(org.apache.flink.table.data.RowData accInput) throws Exception {
    int field$4;
    boolean isNull$4;
    boolean isNull$5;
    int result$6;
    isNull$4 = accInput.isNullAt(0);
    field$4 = -1;
    if (!isNull$4) {
      field$4 = accInput.getInt(0);
    }
    int result$8 = -1;
    boolean isNull$8;
    if (isNull$4) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$8 = agg0_sumIsNull;
      if (!isNull$8) {
        result$8 = agg0_sum;
      }
    }
    else {
      int result$7 = -1;
    boolean isNull$7;
    if (agg0_sumIsNull) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$7 = isNull$4;
      if (!isNull$7) {
        result$7 = field$4;
      }
    }
    else {
    isNull$5 = agg0_sumIsNull || isNull$4;
    result$6 = -1;
    if (!isNull$5) {
    result$6 = (int) (agg0_sum + field$4);
    }
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$7 = isNull$5;
      if (!isNull$7) {
        result$7 = result$6;
      }
    }
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$8 = isNull$7;
      if (!isNull$8) {
        result$8 = result$7;
      }
    }
    agg0_sum = result$8;
    agg0_sumIsNull = isNull$8;
  }
  @Override
  public void retract(org.apache.flink.table.data.RowData retractInput) throws Exception {
    throw new java.lang.RuntimeException("This function not require retract method, but the retract method is called.");
  }
  @Override
  public void merge(Object ns, org.apache.flink.table.data.RowData otherAcc) throws Exception {
    namespace = (java.lang.Long) ns;
    int field$9;
    boolean isNull$9;
    boolean isNull$10;
    int result$11;
    isNull$9 = otherAcc.isNullAt(0);
    field$9 = -1;
    if (!isNull$9) {
      field$9 = otherAcc.getInt(0);
    }
    int result$13 = -1;
    boolean isNull$13;
    if (isNull$9) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$13 = agg0_sumIsNull;
      if (!isNull$13) {
        result$13 = agg0_sum;
      }
    }
    else {
      int result$12 = -1;
    boolean isNull$12;
    if (agg0_sumIsNull) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$12 = isNull$9;
      if (!isNull$12) {
        result$12 = field$9;
      }
    }
    else {
    isNull$10 = agg0_sumIsNull || isNull$9;
    result$11 = -1;
    if (!isNull$10) {
    result$11 = (int) (agg0_sum + field$9);
    }
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$12 = isNull$10;
      if (!isNull$12) {
        result$12 = result$11;
      }
    }
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$13 = isNull$12;
      if (!isNull$13) {
        result$13 = result$12;
      }
    }
    agg0_sum = result$13;;
    agg0_sumIsNull = isNull$13;
  }

  @Override
  public void setAccumulators(Object ns, org.apache.flink.table.data.RowData acc)
  throws Exception {
    namespace = (java.lang.Long) ns;
    int field$3;
    boolean isNull$3;
    isNull$3 = acc.isNullAt(0);
    field$3 = -1;
    if (!isNull$3) {
      field$3 = acc.getInt(0);
    }
    agg0_sum = field$3;
    agg0_sumIsNull = isNull$3;
  }

  @Override
  public org.apache.flink.table.data.RowData getAccumulators() throws Exception {
    acc$2 = new org.apache.flink.table.data.GenericRowData(1);
    if (agg0_sumIsNull) {
      acc$2.setField(0, null);
    } else {
      acc$2.setField(0, agg0_sum);
    }
    return acc$2;
  }
  @Override
  public org.apache.flink.table.data.RowData createAccumulators() throws Exception {
    acc$1 = new org.apache.flink.table.data.GenericRowData(1);
    if (true) {
      acc$1.setField(0, null);
    } else {
      acc$1.setField(0, ((int) -1));
    }
    return acc$1;
  }
  @Override
  public org.apache.flink.table.data.RowData getValue(Object ns) throws Exception {
    namespace = (java.lang.Long) ns;
    aggValue$14 = new org.apache.flink.table.data.GenericRowData(3);
    if (agg0_sumIsNull) {
      aggValue$14.setField(0, null);
    } else {
      aggValue$14.setField(0, agg0_sum);
    }
    aggValue$14.setField(1, org.apache.flink.table.data.TimestampData.fromEpochMillis(windowAssigner$0.getWindowStart(namespace)));
    aggValue$14.setField(2, org.apache.flink.table.data.TimestampData.fromEpochMillis(namespace));
    return aggValue$14;
  }
  @Override
  public void cleanup(Object ns) throws Exception {
    namespace = (java.lang.Long) ns;
  }
  @Override
  public void close() throws Exception {}
}
```


### [Exchange distribution](WVFExample2.java)
`distribution=[hash[name]]`
```markdown
      +- Exchange(distribution=[hash[name]])
```


## HOP 滑动窗口


## ProcessTableFunction
### [PTF](PTFExample.java)

> [2]:ProcessTableFunction(invocation=[f(TABLE(#0), 20, DEFAULT())], uid=[null], select=[name,score], rowType=[RecordType(VARCHAR(2147483647) name, INTEGER score)])
- ProcessTableOperatorFactory
  - GeneratedProcessTableRunner
```java
public final class ProcessTableRunner$5
    extends org.apache.flink.table.runtime.generated.ProcessTableRunner {
  private transient org.apache.flink.table.data.RowData[] tables;
  private transient org.apache.flink.table.data.conversion.RowRowConverter converter$0;
  private transient org.apache.flink.table.examples.java.my.PTFExample$Function function_org$apache$flink$table$examples$java$my$PTFExample$Function;
  private transient org.apache.flink.table.data.conversion.RowRowConverter converter$2;
  private TableFunctionResultConverterCollector$3 resultConverterCollector$4 = null;

  public ProcessTableRunner$5(Object[] references) throws Exception {
    converter$0 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[0]));
    function_org$apache$flink$table$examples$java$my$PTFExample$Function = (((org.apache.flink.table.examples.java.my.PTFExample$Function) references[1]));
    converter$2 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[2]));
  }

  @Override
  public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
    converter$0.open(getRuntimeContext().getUserCodeClassLoader());
    function_org$apache$flink$table$examples$java$my$PTFExample$Function.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
    converter$2.open(getRuntimeContext().getUserCodeClassLoader());
    resultConverterCollector$4 = new TableFunctionResultConverterCollector$3();
    resultConverterCollector$4.setRuntimeContext(getRuntimeContext());
    resultConverterCollector$4.open(new org.apache.flink.api.common.functions.DefaultOpenContext());
    function_org$apache$flink$table$examples$java$my$PTFExample$Function.setCollector(resultConverterCollector$4);
    resultConverterCollector$4.setCollector(runnerCollector);
    tables = new org.apache.flink.table.data.RowData[1];
  }

  @Override
  public void processElement(
      org.apache.flink.table.data.RowData[] stateToFunction,
      boolean[] stateCleared,
      org.apache.flink.table.data.RowData[] stateFromFunction,
      int inputIndex,
      org.apache.flink.table.data.RowData row) throws Exception {
    java.util.Arrays.fill(tables, null);
    tables[inputIndex] = row;
    function_org$apache$flink$table$examples$java$my$PTFExample$Function.eval(tables[0] == null ? null : ((org.apache.flink.types.Row) converter$0.toExternal((org.apache.flink.table.data.RowData) tables[0])), false ? null : ((java.lang.Integer) ((int) 20)));
  }

  @Override
  public void close() throws Exception {
    function_org$apache$flink$table$examples$java$my$PTFExample$Function.close();
  }
  
  public class TableFunctionResultConverterCollector$3 extends org.apache.flink.table.runtime.collector.WrappingCollector {
    public TableFunctionResultConverterCollector$3() throws Exception {}
    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {}
    @Override
    public void collect(Object record) throws Exception {
      org.apache.flink.table.data.RowData externalResult$1 = (org.apache.flink.table.data.RowData) (org.apache.flink.table.data.RowData) converter$2.toInternalOrNull((org.apache.flink.types.Row) record);
      if (externalResult$1 != null) {
        outputResult(externalResult$1);
      }
    }
    @Override
    public void close() {}
  }
}
```

### [PTF partition by](PTFExample2.java)
```markdown
Sink(table=[default_catalog.default_database.MySink], fields=[name])
+- Calc(select=[name])
   +- ProcessTableFunction(invocation=[f(TABLE(#0) PARTITION BY($0), 20, DEFAULT())], uid=[f], select=[name,name0,score], rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) name0, INTEGER score)])
      +- Exchange(distribution=[hash[name]])
         +- TableSourceScan(table=[[default_catalog, default_database, t]], fields=[name, score])
```

### [PTF state](PTFExample3.java)

logical:
```markdown
LogicalSink(table=[default_catalog.default_database.MySink], fields=[name, pv])
+- LogicalProject(inputs=[0], exprs=[[$2]])
   +- LogicalTableFunctionScan(invocation=[f(TABLE(#0) PARTITION BY($0), 20, DEFAULT())], rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) name0, INTEGER pv)])
      +- LogicalProject(inputs=[0..1])
         +- LogicalTableScan(table=[[default_catalog, default_database, t]])

FlinkLogicalSink(table=[default_catalog.default_database.MySink], fields=[name, pv])
    +- FlinkLogicalCalc(select=[name, pv])
        +- FlinkLogicalTableFunctionScan(invocation=[f(TABLE(#0) PARTITION BY($0), 20, DEFAULT())], rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) name0, INTEGER pv)])
            +- FlinkLogicalTableSourceScan(table=[[default_catalog, default_database, t]], fields=[name, score])
```
optimize:
```markdown
StreamPhysicalSink(table=[default_catalog.default_database.MySink], fields=[name, pv])
    StreamPhysicalCalc(select=[name, pv])
        StreamPhysicalProcessTableFunction(invocation=[f(TABLE(#0) PARTITION BY($0), 20, DEFAULT())], uid=[f], select=[name,name0,pv], rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) name0, INTEGER pv)])
            StreamPhysicalExchange(distribution=[hash[name]])
                StreamPhysicalTableSourceScan(table=[[default_catalog, default_database, t]], fields=[name, score])
```
physical:
```markdown
Sink(table=[default_catalog.default_database.MySink], fields=[name, pv])
    +- Calc(select=[name, pv])
        +- ProcessTableFunction(invocation=[f(TABLE(#0) PARTITION BY($0), 20, DEFAULT())], uid=[f], select=[name,name0,pv], rowType=[RecordType(VARCHAR(2147483647) name, VARCHAR(2147483647) name0, INTEGER pv)])
            +- Exchange(distribution=[hash[name]])
                +- TableSourceScan(table=[[default_catalog, default_database, t]], fields=[name, score])
```

moveStateToFunction -> state -> moveStateFromFunction

```java
import org.apache.flink.table.examples.java.my.PTFExample3_State;

public final class ProcessTableRunner$7 extends org.apache.flink.table.runtime.generated.ProcessTableRunner {
    private transient org.apache.flink.table.data.RowData[] tables;
    private transient org.apache.flink.table.data.conversion.StructuredObjectConverter converter$1;
    private transient org.apache.flink.table.data.conversion.RowRowConverter converter$2;
    private transient org.apache.flink.table.examples.java.my.PTFExample3$Function function_org$apache$flink$table$examples$java$my$PTFExample3$Function;
    private transient org.apache.flink.table.data.conversion.RowRowConverter converter$4;
    private TableFunctionResultConverterCollector$5 resultConverterCollector$6 = null;

    public ProcessTableRunner$7(Object[] references) throws Exception {
        converter$1 = (((org.apache.flink.table.data.conversion.StructuredObjectConverter) references[0]));
        converter$2 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[1]));
        function_org$apache$flink$table$examples$java$my$PTFExample3$Function = (((org.apache.flink.table.examples.java.my.PTFExample3$Function) references[2]));
        converter$4 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[3]));
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        converter$1.open(getRuntimeContext().getUserCodeClassLoader());
        converter$2.open(getRuntimeContext().getUserCodeClassLoader());
        function_org$apache$flink$table$examples$java$my$PTFExample3$Function.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
        converter$4.open(getRuntimeContext().getUserCodeClassLoader());
        resultConverterCollector$6 = new TableFunctionResultConverterCollector$5();
        resultConverterCollector$6.setRuntimeContext(getRuntimeContext());
        resultConverterCollector$6.open(new org.apache.flink.api.common.functions.DefaultOpenContext());
        function_org$apache$flink$table$examples$java$my$PTFExample3$Function.setCollector(resultConverterCollector$6);
        resultConverterCollector$6.setCollector(runnerCollector);
        tables = new org.apache.flink.table.data.RowData[1];
    }

    @Override
    public void processElement(
            org.apache.flink.table.data.RowData[] stateToFunction,
            boolean[] stateCleared,
            org.apache.flink.table.data.RowData[] stateFromFunction,
            int inputIndex,
            org.apache.flink.table.data.RowData row) throws Exception {
        java.util.Arrays.fill(tables, null);
        tables[inputIndex] = row;
        final PTFExample3_State.Function.Count externalState$0;
        if (stateToFunction[0] == null) {
            externalState$0 = new PTFExample3_State.Function.Count();
        } else {
            externalState$0 = (PTFExample3_State.Function.Count) converter$1.toExternal((org.apache.flink.table.data.RowData) stateToFunction[0]);
        }
        function_org$apache$flink$table$examples$java$my$PTFExample3$Function.eval(runnerContext, externalState$0, tables[0] == null ? null : ((org.apache.flink.types.Row) converter$2.toExternal((org.apache.flink.table.data.RowData) tables[0])), false ? null : ((java.lang.Integer) ((int) 20)));
        stateFromFunction[0] = stateCleared[0] ? null : (org.apache.flink.table.data.RowData) converter$1.toInternalOrNull((PTFExample3_State.Function.Count) externalState$0);
    }

    @Override
    public void close() throws Exception {
        function_org$apache$flink$table$examples$java$my$PTFExample3$Function.close();
    }


    public class TableFunctionResultConverterCollector$5 extends org.apache.flink.table.runtime.collector.WrappingCollector {
        public TableFunctionResultConverterCollector$5() throws Exception {
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        }

        @Override
        public void collect(Object record) throws Exception {
            org.apache.flink.table.data.RowData externalResult$3 = (org.apache.flink.table.data.RowData) (org.apache.flink.table.data.RowData) converter$4.toInternalOrNull((org.apache.flink.types.Row) record);
            if (externalResult$3 != null) {
                outputResult(externalResult$3);
            }
        }

        @Override
        public void close() {
        }
    }
}
```

# KeyedProcessOperator
### AggsHandleFunction
```java
public final class GroupAggsHandler$9 implements org.apache.flink.table.runtime.generated.AggsHandleFunction {
  int agg0_sum;
  boolean agg0_sumIsNull;
  org.apache.flink.table.data.GenericRowData acc$0 = new org.apache.flink.table.data.GenericRowData(1);
  org.apache.flink.table.data.GenericRowData acc$1 = new org.apache.flink.table.data.GenericRowData(1);
  org.apache.flink.table.data.GenericRowData aggValue$8 = new org.apache.flink.table.data.GenericRowData(1);
  private org.apache.flink.table.runtime.dataview.StateDataViewStore store;

  public GroupAggsHandler$9(java.lang.Object[] references) throws Exception {}

  private org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {
    return store.getRuntimeContext();
  }

  @Override
  public void open(org.apache.flink.table.runtime.dataview.StateDataViewStore store) throws Exception {
    this.store = store;
  }

  @Override
  public void setWindowSize(int windowSize) {}

  @Override
  public void accumulate(org.apache.flink.table.data.RowData accInput) throws Exception {
    int field$3;
    boolean isNull$3;
    boolean isNull$4;
    int result$5;
    isNull$3 = accInput.isNullAt(1);
    field$3 = -1;
    if (!isNull$3) {
      field$3 = accInput.getInt(1);
    }
    int result$7 = -1;
    boolean isNull$7;
    if (isNull$3) {
     // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
     // --- End cast section
      isNull$7 = agg0_sumIsNull;
      if (!isNull$7) {
        result$7 = agg0_sum;
      }
    } else {
      int result$6 = -1;
      boolean isNull$6;
      if (agg0_sumIsNull) {
       // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
       // --- End cast section
        isNull$6 = isNull$3;
        if (!isNull$6) {
          result$6 = field$3;
        }
      } else {
        isNull$4 = agg0_sumIsNull || isNull$3;
        result$5 = -1;
        if (!isNull$4) {
            result$5 = (int) (agg0_sum + field$3);
        }
        // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
        // --- End cast section
        isNull$6 = isNull$4;
        if (!isNull$6) {
          result$6 = result$5;
        }
      }
      // --- Cast section generated by org.apache.flink.table.planner.functions.casting.IdentityCastRule
      // --- End cast section
      isNull$7 = isNull$6;
      if (!isNull$7) {
        result$7 = result$6;
      }
    }
    agg0_sum = result$7;
    agg0_sumIsNull = isNull$7;
  }

  @Override
  public void retract(org.apache.flink.table.data.RowData retractInput) throws Exception {
    throw new java.lang.RuntimeException("This function not require retract method, but the retract method is called.");
  }

  @Override
  public void merge(org.apache.flink.table.data.RowData otherAcc) throws Exception {
    throw new java.lang.RuntimeException("This function not require merge method, but the merge method is called.");
  }

  @Override
  public void resetAccumulators() throws Exception {
    agg0_sum = ((int) -1);
    agg0_sumIsNull = true;
  }

  @Override
  public org.apache.flink.table.data.RowData getAccumulators() throws Exception {
    acc$1 = new org.apache.flink.table.data.GenericRowData(1);
    if (agg0_sumIsNull) {
      acc$1.setField(0, null);
    } else {
      acc$1.setField(0, agg0_sum);
    }
    return acc$1;
  }

  @Override
  public void setAccumulators(org.apache.flink.table.data.RowData acc) throws Exception {
    int field$2;
    boolean isNull$2;
    isNull$2 = acc.isNullAt(0);
    field$2 = -1;
    if (!isNull$2) {
      field$2 = acc.getInt(0);
    }
    agg0_sum = field$2;
    agg0_sumIsNull = isNull$2;
  }

  @Override
  public org.apache.flink.table.data.RowData createAccumulators() throws Exception {
    acc$0 = new org.apache.flink.table.data.GenericRowData(1);
    if (true) {
      acc$0.setField(0, null);
    } else {
      acc$0.setField(0, ((int) -1));
    }
    return acc$0;
  }

  @Override
  public org.apache.flink.table.data.RowData getValue() throws Exception {
    aggValue$8 = new org.apache.flink.table.data.GenericRowData(1);
    if (agg0_sumIsNull) {
      aggValue$8.setField(0, null);
    } else {
      aggValue$8.setField(0, agg0_sum);
    }
    return aggValue$8;
  }
  @Override
  public void cleanup() throws Exception {}
  @Override
  public void close() throws Exception {}
}
```

# [DESCRIPTOR](PTFExample4.java)
```java
public final class ProcessTableRunner$7 extends org.apache.flink.table.runtime.generated.ProcessTableRunner {
    private transient org.apache.flink.table.examples.java.my.PTFExample4$Function function_org$apache$flink$table$examples$java$my$PTFExample4$Function;
    private transient org.apache.flink.table.data.conversion.RowRowConverter converter$1;
    private TableFunctionResultConverterCollector$2 resultConverterCollector$3 = null;
    private final org.apache.flink.table.data.binary.BinaryStringData str$4 = org.apache.flink.table.data.binary.BinaryStringData.fromString("score");
    private transient org.apache.flink.types.ColumnList columnList$5;
    private transient org.apache.flink.table.data.conversion.RowRowConverter converter$6;
    public ProcessTableRunner$7(Object[] references) throws Exception {
        function_org$apache$flink$table$examples$java$my$PTFExample4$Function = (((org.apache.flink.table.examples.java.my.PTFExample4$Function) references[0]));
        converter$1 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[1]));
        columnList$5 = (((org.apache.flink.types.ColumnList) references[2]));
        converter$6 = (((org.apache.flink.table.data.conversion.RowRowConverter) references[3]));
    }
    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        function_org$apache$flink$table$examples$java$my$PTFExample4$Function.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
        converter$1.open(getRuntimeContext().getUserCodeClassLoader());
        resultConverterCollector$3 = new TableFunctionResultConverterCollector$2();
        resultConverterCollector$3.setRuntimeContext(getRuntimeContext());
        resultConverterCollector$3.open(new org.apache.flink.api.common.functions.DefaultOpenContext());
        function_org$apache$flink$table$examples$java$my$PTFExample4$Function.setCollector(resultConverterCollector$3);
        converter$6.open(getRuntimeContext().getUserCodeClassLoader());
    }
    @Override
    public void callEval() throws Exception {
        resultConverterCollector$3.setCollector(evalCollector);
        function_org$apache$flink$table$examples$java$my$PTFExample4$Function.eval(
                inputIndex != 0 ? null : ((org.apache.flink.types.Row) converter$6.toExternal((org.apache.flink.table.data.RowData) inputRow)), 
                false ? null : ((org.apache.flink.types.ColumnList) columnList$5), 
                false ? null : ((java.lang.Integer) ((int) 20))
        );
    }
    @Override
    public void callOnTimer() throws Exception {}
    @Override
    public void close() throws Exception {
        function_org$apache$flink$table$examples$java$my$PTFExample4$Function.close();
    }

    public class TableFunctionResultConverterCollector$2 extends org.apache.flink.table.runtime.collector.WrappingCollector {
        public TableFunctionResultConverterCollector$2() throws Exception {}
        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {}
        @Override
        public void collect(Object record) throws Exception {
            org.apache.flink.table.data.RowData externalResult$0 = 
                    (org.apache.flink.table.data.RowData) 
                            converter$1.toInternalOrNull((org.apache.flink.types.Row) record);
            if (externalResult$0 != null) {
                outputResult(externalResult$0);
            }
        }
        @Override
        public void close() {}
    }
}
```


