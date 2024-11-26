package com.gruelbox.transactionoutbox.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.springframework.core.ParameterNameDiscoverer;

public class TestSpringExpressionTopicGenerator {
  @Test
  void testSimpleParameter() {
    Object[] args = {13L, "TestParamValue", null};
    String[] parameterNames = {"longParam", "stringParam", "nullParam"};
    doTest(args, parameterNames, "#longParam", "13");
    doTest(args, parameterNames, "#stringParam", "TestParamValue");
    doTest(args, parameterNames, "#nullParam", "null");
  }

  @Test
  void testSimpleParametersCombination() {
    Object[] args = {13L, "TestParamValue", null};
    String[] parameterNames = {"longParam", "stringParam", "nullParam"};

    doTest(
        args,
        parameterNames,
        "'prefix-' + #longParam + '-' + #stringParam + '-' +#nullParam + '-suffix'",
        "prefix-13-TestParamValue-null-suffix");
  }

  @Test
  void testComplexParameter() {
    Object[] args = {
      new TestObjectParameter("testName1", new TestInnerObject("testInnerName")),
      new TestObjectParameter("testName2", null),
      null
    };
    String[] parameterNames = {"param1", "param2", "param3"};

    doTest(args, parameterNames, "#param1.name", "testName1");
    doTest(args, parameterNames, "#param1.value.innerName", "testInnerName");

    doTest(args, parameterNames, "#param1?.name", "testName1");
    doTest(args, parameterNames, "#param1?.value?.innerName", "testInnerName");

    doTest(args, parameterNames, "#param2?.name", "testName2");
    doTest(args, parameterNames, "#param2?.value?.innerName", "null");

    doTest(args, parameterNames, "#param3?.name", "null");
    doTest(args, parameterNames, "#param3?.value?.innerName", "null");
  }

  @Test
  void testComplexParameterCombination() {
    Object[] args = {
      new TestObjectParameter("testName1", new TestInnerObject("testInnerName")),
      new TestObjectParameter("testName2", null),
      null
    };
    String[] parameterNames = {"param1", "param2", "param3"};

    doTest(
        args,
        parameterNames,
        "#param1?.name + '-' + #param1?.value?.innerName + ':' + #param2?.name + '-' + #param2?.value?.innerName + "
            + "':' + #param3?.name + '-' + #param3?.value?.innerName",
        "testName1-testInnerName:testName2-null:null-null");
  }

  @Data
  private static class TestObjectParameter {
    private final String name;
    private final TestInnerObject value;
  }

  @Data
  private static class TestInnerObject {
    private final String innerName;
  }

  private void doTest(Object[] args, String[] parameterNames, String expression, String expected) {
    ParameterNameDiscoverer pndMock = mock(ParameterNameDiscoverer.class);
    when(pndMock.getParameterNames((Method) any())).thenReturn(parameterNames);

    SpringExpressionTopicGenerator topicGenerator =
        new SpringExpressionTopicGenerator(pndMock, "TestBean");
    String result = topicGenerator.generateTopic(false, expression, mock(Method.class), args);

    assertThat(result).isEqualTo(expected);
  }
}
