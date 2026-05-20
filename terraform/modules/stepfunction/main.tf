resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "ETL_Taller2"
  role_arn = var.sfn_role_arn

  definition = jsonencode({
    Comment = "Pipeline con Quality Gate previo (Great Expectations)"
    StartAt = "quality_check"
    States = {
      quality_check = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "Great_Expectations"
        }
        Next = "bronze_to_silver"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "quality_failed"
          }
        ]
      }
      bronze_to_silver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "bronze_to_silver"
        }
        Next = "silver_to_gold"
      }
      silver_to_gold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = "silver_to_gold"
        }
        End = true
      }
      quality_failed = {
        Type  = "Fail"
        Error = "DataQualityFailed"
        Cause = "El job Great_Expectations falló. El pipeline se detuvo."
      }
    }
  })

  tags = var.tags
}