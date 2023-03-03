package com.hzsun.zbp.iot.trigger;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

// 不设置触发器默认 窗口末尾，才计算 且不会清空窗口数据
//决定了何时启动Window Function来处理窗口中的数据以及何时将窗口内的数据清理。
public class OneByOneTrigger extends Trigger<Object, TimeWindow> {

//    private final ReducingStateDescriptor<Long> timeStateDesc =
//            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    //定义状态变量 用于接受数据
    //private final ReducingStateDescriptor<Long> stateDesc =
    //        new ReducingStateDescriptor<>("count", new OneByOneTrigger.Sum(), LongSerializer.INSTANCE);


    //private long interval;
    //private OneByOneTrigger(long interval){
    //    this.interval =interval;
    //}
    //
    //public OneByOneTrigger(){
    //    super();
    //}


    //public  static OneByOneTrigger create(){
    //    return  new OneByOneTrigger();
    //}


//    private ReducingStateDescriptor<Long> countStateDescriptor = new ReducingStateDescriptor("counter", new Sum(), LongSerializer.INSTANCE);

    //@Override
    //public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
    //
    //    ReducingState<Long> fireTimestamp = triggerContext.getPartitionedState(stateDesc);
    //
    //
    //
    //    //       触发
    //    return TriggerResult.FIRE;
    //}

//每次数据进入该window时都会触发
    @Override
    public TriggerResult onElement(Object  morningCheckTopDTO, long l, TimeWindow window, TriggerContext ctx) throws Exception {
        //if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        //} else {
        //    ctx.registerEventTimeTimer(window.maxTimestamp());
        //    return TriggerResult.CONTINUE;
        //}





        //return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    //trigger注册的时间达到时触发
    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        ValueState<Boolean> first = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("first", Types.BOOLEAN));

        first.clear();

    }

//    @Override
//    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
////        啥也不干
//        return TriggerResult.CONTINUE;
//    }
//
//    @Override
//    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
//        return TriggerResult.CONTINUE;
//    }
//
//    @Override
//    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
//        System.out.println("clear ....");

        //TriggerResult purge = TriggerResult.PURGE;



        //ReducingState<Long> fireTimestamp = triggerContext.getPartitionedState(stateDesc);

//        triggerContext.getPartitionedState(stateDesc);
//



        //triggerContext.deleteEventTimeTimer(timeWindow.maxTimestamp());

        //fireTimestamp.clear();

    //}

    //
    //public static Trigger<? super MorningCheckListDTO, ? super TimeWindow> create(){
    //
    //    return new OneByOneTrigger();
    //}


    //private static class Sum implements ReduceFunction<Long> {
    //    private static final long serialVersionUID = 1L;
    //
    //    @Override
    //    public Long reduce(Long value1, Long value2) throws Exception {
    //        return value1 + value2;
    //    }
    //
    //
    //}
}
