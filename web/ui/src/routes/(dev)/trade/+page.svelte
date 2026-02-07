<script lang="ts">
  import * as m from '$lib/paraglide/messages.js'
  import { getLocale, localizeHref } from '$lib/paraglide/runtime.js'
</script>

<main class="flex-1 flex items-center justify-center">
  <div class="max-w-4xl w-full px-4 py-8">
    <div class="steps-container relative flex flex-col gap-4">
      <!-- 垂直连接线 -->
      <div class="absolute left-[1rem] top-[40px] bottom-[40px] w-[0.5rem] bg-base-300"></div>

      <!-- Download bin -->
      <div class="step-item flex items-start relative">
        <div class="step-circle-container flex-none relative">
          <div class="step-circle w-10 h-10 rounded-full bg-primary flex items-center justify-center text-primary-content font-bold">
            1
          </div>
        </div>
        <div class="step-content flex gap-8 items-start flex-1">
          <h3 class="step-title text-xl font-bold w-[13rem]">{m.compile()}</h3>
          <div class="content-block flex-1 p-6 bg-base-200 rounded-lg">
            <p>{m.trade_compile()}</p>
            <a href={localizeHref("/setting?tab=build")} class="btn btn-primary mt-2 btn-outline btn-sm">
              {m.go_compile()}
            </a>
          </div>
        </div>
      </div>

      <!-- DataBase -->
      <div class="step-item flex items-start relative">
        <div class="step-circle-container flex-none relative">
          <div class="step-circle w-10 h-10 rounded-full bg-primary flex items-center justify-center text-primary-content font-bold">
            2
          </div>
        </div>
        <div class="step-content flex gap-8 items-start flex-1">
          <h3 class="step-title text-xl font-bold w-[13rem]">{m.database()}</h3>
          <div class="content-block flex-1 p-6 bg-base-200 rounded-lg">
            <p>{m.database_desc()}</p>
            <a href="https://questdb.io/docs/" class="btn btn-primary mt-2 btn-outline btn-sm">{m.document()}</a>
          </div>
        </div>
      </div>

      <!-- Edit Config -->
      <div class="step-item flex items-start relative">
        <div class="step-circle-container flex-none relative">
          <div class="step-circle w-10 h-10 rounded-full bg-primary flex items-center justify-center text-primary-content font-bold">
            3
          </div>
        </div>
        <div class="step-content flex gap-8 items-start flex-1">
          <h3 class="step-title text-xl font-bold w-[13rem]">{m.edit_config()}</h3>
          <div class="content-block flex-1 p-6 bg-base-200 rounded-lg">
            <p>{m.trade_edit_config_desc()}</p>
            <a href="https://docs.banbot.site/{getLocale()}/guide/configuration.html" target="_blank" class="btn btn-primary mt-2 btn-outline btn-sm">{m.document()}</a>
          </div>
        </div>
      </div>

      <!-- Run Spider & Trader -->
      <div class="step-item flex items-start relative">
        <div class="step-circle-container flex-none relative">
          <div class="step-circle w-10 h-10 rounded-full bg-primary flex items-center justify-center text-primary-content font-bold">
            4
          </div>
        </div>
        <div class="step-content flex gap-8 items-start flex-1">
          <h3 class="step-title text-xl font-bold w-[13rem]">{m.run_banbot()}</h3>
          <div class="content-block flex-1 p-6 bg-base-200 rounded-lg">
            <p>{m.run_banbot_desc()}</p>
          </div>
        </div>
      </div>

      <!-- DashBoard -->
      <div class="step-item flex items-start relative">
        <div class="step-circle-container flex-none relative">
          <div class="step-circle w-10 h-10 rounded-full bg-primary flex items-center justify-center text-primary-content font-bold">
            5
          </div>
        </div>
        <div class="step-content flex gap-8 items-start flex-1">
          <h3 class="step-title text-xl font-bold w-[13rem]">{m.dashboard()}</h3>
          <div class="content-block flex-1 p-6 bg-base-200 rounded-lg">
            <p>{m.dashboard_desc()}</p>
          </div>
        </div>
      </div>
    </div>
    <div class="flex mt-7 justify-center">
      <a class="link" href="https://docs.banbot.site/{getLocale()}/guide/live_trading" target="_blank">{m.document()}</a>
    </div>
  </div>
</main>

<style>
  .step-circle-container {
    width: 40px;
    display: flex;
    justify-content: center;
  }

  .step-circle {
    z-index: 10;
  }
  .step-title{
    margin-left: 1rem;
    line-height: 2.5rem;
  }

</style>
